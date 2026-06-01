use async_trait::async_trait;
use reqwest::Client;
use serde_json::json;
use tracing::{debug, error, warn};

use crate::shared::workflow::TaskType;
use crate::task::entity::task_definition::{LlmResponseFormat, LlmTemplate, TaskInstanceEntity, TaskTemplate};
use crate::task::interface::{TaskExecutionResult, TaskExecutor};
use crate::workflow::entity::workflow_definition::NodeExecutionStatus;

pub struct LlmTaskExecutor {
    client: Client,
}

impl LlmTaskExecutor {
    pub fn new() -> Self {
        Self {
            client: Client::new(),
        }
    }

    fn build_request_body(
        model: &str,
        system_prompt: &str,
        user_prompt: &str,
        temperature: Option<f64>,
        max_tokens: Option<u32>,
        response_format: &Option<LlmResponseFormat>,
    ) -> serde_json::Value {
        let mut messages = vec![];
        if !system_prompt.is_empty() {
            messages.push(json!({"role": "system", "content": system_prompt}));
        }
        messages.push(json!({"role": "user", "content": user_prompt}));

        let mut body = json!({
            "model": model,
            "messages": messages,
        });

        if let Some(temp) = temperature {
            body["temperature"] = json!(temp);
        }
        if let Some(max_tok) = max_tokens {
            body["max_tokens"] = json!(max_tok);
        }
        if let Some(LlmResponseFormat::JsonObject) = response_format {
            body["response_format"] = json!({"type": "json_object"});
        }

        body
    }

    fn build_input_snapshot(
        base_url: &str,
        model: &str,
        system_prompt: &str,
        user_prompt: &str,
        config: &LlmTemplate,
    ) -> serde_json::Value {
        json!({
            "base_url": base_url,
            "model": model,
            "system_prompt": system_prompt,
            "user_prompt": user_prompt,
            "api_key_ref": config.api_key_ref,
            "temperature": config.temperature,
            "max_tokens": config.max_tokens,
            "response_format": config.response_format,
        })
    }

    fn try_build_success_output(
        resp_json: &serde_json::Value,
        model: &str,
        response_format: &Option<LlmResponseFormat>,
        attempt: u32,
    ) -> Result<serde_json::Value, String> {
        let content = resp_json
            .pointer("/choices/0/message/content")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let finish_reason = resp_json
            .pointer("/choices/0/finish_reason")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();

        let usage = resp_json.get("usage").cloned().unwrap_or(json!(null));
        let resp_model = resp_json
            .get("model")
            .and_then(|v| v.as_str())
            .unwrap_or(model)
            .to_string();

        let mut output = json!({
            "content": content,
            "usage": usage,
            "model": resp_model,
            "finish_reason": finish_reason,
            "attempt": attempt + 1,
        });

        if matches!(response_format, Some(LlmResponseFormat::JsonObject)) {
            match serde_json::from_str::<serde_json::Value>(&content) {
                Ok(v) => {
                    output["parsed"] = v;
                }
                Err(e) => {
                    return Err(format!(
                        "response_format=JsonObject but content is not valid JSON: {}",
                        e
                    ));
                }
            }
        }

        Ok(output)
    }
}

#[async_trait]
impl TaskExecutor for LlmTaskExecutor {
    async fn execute_task(
        &self,
        task_instance: &TaskInstanceEntity,
    ) -> anyhow::Result<TaskExecutionResult> {
        let config = match &task_instance.task_template {
            TaskTemplate::Llm(c) => c,
            other => {
                error!(
                    task_instance_id = %task_instance.task_instance_id,
                    template = ?other,
                    "expected Llm config"
                );
                return Err(anyhow::anyhow!("Expected Llm config but got other"));
            }
        };

        let input = task_instance
            .input
            .as_ref()
            .cloned()
            .unwrap_or_else(|| json!({}));

        let system_prompt = input
            .get("system_prompt")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let user_prompt = input
            .get("user_prompt")
            .and_then(|v| v.as_str())
            .unwrap_or(&config.user_prompt);
        let api_key = input.get("_api_key").and_then(|v| v.as_str()).unwrap_or("");

        let base_url = input
            .get("base_url")
            .and_then(|v| v.as_str())
            .unwrap_or(&config.base_url);
        let model = input
            .get("model")
            .and_then(|v| v.as_str())
            .unwrap_or(&config.model);

        let url = format!("{}/chat/completions", base_url.trim_end_matches('/'));

        let body = Self::build_request_body(
            model,
            system_prompt,
            user_prompt,
            config.temperature,
            config.max_tokens,
            &config.response_format,
        );
        let input_snapshot =
            Self::build_input_snapshot(base_url, model, system_prompt, user_prompt, config);

        let mut last_error: Option<String> = None;
        let attempts = config.retry_count + 1;

        for attempt in 0..attempts {
            let mut request = self
                .client
                .post(&url)
                .header("Content-Type", "application/json")
                .json(&body);

            if !api_key.is_empty() {
                request = request.header("Authorization", format!("Bearer {}", api_key));
            }

            if config.timeout > 0 {
                request = request.timeout(std::time::Duration::from_secs(config.timeout as u64));
            }

            match request.send().await {
                Ok(resp) => {
                    let status_code = resp.status().as_u16();
                    let resp_body = resp.text().await.unwrap_or_default();

                    if (200..300).contains(&status_code) {
                        match serde_json::from_str::<serde_json::Value>(&resp_body) {
                            Ok(resp_json) => {
                                match Self::try_build_success_output(
                                    &resp_json,
                                    model,
                                    &config.response_format,
                                    attempt,
                                ) {
                                    Ok(output) => {
                                        debug!(
                                            task_instance_id = %task_instance.task_instance_id,
                                            model = %resp_json.get("model").and_then(|v| v.as_str()).unwrap_or(model),
                                            finish_reason = %resp_json.pointer("/choices/0/finish_reason").and_then(|v| v.as_str()).unwrap_or("unknown"),
                                            "LLM request succeeded"
                                        );

                                        return Ok(TaskExecutionResult {
                                            status: NodeExecutionStatus::Success,
                                            input: Some(input_snapshot),
                                            output: Some(output),
                                            error_message: None,
                                        });
                                    }
                                    Err(e) => {
                                        warn!(
                                            task_instance_id = %task_instance.task_instance_id,
                                            attempt = attempt + 1,
                                            error = %e,
                                            "LLM returned non-JSON content with JsonObject format"
                                        );
                                        last_error = Some(e);
                                        if attempt < config.retry_count && config.retry_delay > 0 {
                                            tokio::time::sleep(
                                                std::time::Duration::from_secs(
                                                    config.retry_delay as u64,
                                                ),
                                            )
                                            .await;
                                        }
                                        continue;
                                    }
                                }
                            }
                            Err(e) => {
                                warn!(
                                    task_instance_id = %task_instance.task_instance_id,
                                    status_code = status_code,
                                    error = %e,
                                    "LLM response is not valid JSON"
                                );
                                last_error = Some(format!("LLM response parse error: {}", e));
                            }
                        }
                    } else if status_code == 429 {
                        warn!(
                            task_instance_id = %task_instance.task_instance_id,
                            attempt = attempt + 1,
                            "LLM rate limited (429)"
                        );
                        last_error = Some(format!("Rate limited (429): {}", resp_body));
                    } else if (400..500).contains(&status_code) && status_code != 429 {
                        error!(
                            task_instance_id = %task_instance.task_instance_id,
                            status_code = status_code,
                            "LLM client error (4xx), not retrying"
                        );
                        return Ok(TaskExecutionResult {
                            status: NodeExecutionStatus::Failed,
                            input: Some(input_snapshot),
                            output: None,
                            error_message: Some(format!(
                                "LLM API error {}: {}",
                                status_code, resp_body
                            )),
                        });
                    } else {
                        warn!(
                            task_instance_id = %task_instance.task_instance_id,
                            status_code = status_code,
                            attempt = attempt + 1,
                            "LLM server error"
                        );
                        last_error = Some(format!("LLM API error {}: {}", status_code, resp_body));
                    }
                }
                Err(e) => {
                    warn!(
                        task_instance_id = %task_instance.task_instance_id,
                        attempt = attempt + 1,
                        error = %e,
                        "LLM request failed"
                    );
                    last_error = Some(e.to_string());
                }
            }

            if attempt < config.retry_count && config.retry_delay > 0 {
                tokio::time::sleep(std::time::Duration::from_secs(config.retry_delay as u64)).await;
            }
        }

        let error_msg = last_error.unwrap_or_else(|| "Unknown error".to_string());
        error!(
            task_instance_id = %task_instance.task_instance_id,
            url = %url,
            attempts = attempts,
            error = %error_msg,
            "LLM task failed after all retries"
        );

        Ok(TaskExecutionResult {
            status: NodeExecutionStatus::Failed,
            input: Some(input_snapshot),
            output: None,
            error_message: Some(error_msg),
        })
    }

    fn task_type(&self) -> TaskType {
        TaskType::Llm
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_build_request_body_basic() {
        let body = LlmTaskExecutor::build_request_body(
            "gpt-4",
            "You are helpful",
            "Hello",
            None,
            None,
            &None,
        );
        assert_eq!(body["model"], "gpt-4");
        let messages = body["messages"].as_array().unwrap();
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0]["role"], "system");
        assert_eq!(messages[0]["content"], "You are helpful");
        assert_eq!(messages[1]["role"], "user");
        assert_eq!(messages[1]["content"], "Hello");
    }

    #[test]
    fn test_build_request_body_no_system_prompt() {
        let body = LlmTaskExecutor::build_request_body(
            "gpt-4",
            "",
            "Hi",
            None,
            None,
            &None,
        );
        let messages = body["messages"].as_array().unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0]["role"], "user");
    }

    #[test]
    fn test_build_request_body_with_options() {
        let body = LlmTaskExecutor::build_request_body(
            "gpt-4",
            "sys",
            "user",
            Some(0.7),
            Some(256),
            &Some(LlmResponseFormat::JsonObject),
        );
        assert_eq!(body["temperature"], 0.7);
        assert_eq!(body["max_tokens"], 256);
        assert_eq!(body["response_format"]["type"], "json_object");
    }

    #[test]
    fn test_build_input_snapshot() {
        let config = LlmTemplate {
            api_key_ref: "key-1".into(),
            base_url: "https://api.example.com".into(),
            model: "gpt-4".into(),
            system_prompt: None,
            user_prompt: "hello".into(),
            temperature: Some(0.5),
            max_tokens: Some(100),
            timeout: 30,
            retry_count: 1,
            retry_delay: 2,
            response_format: Some(LlmResponseFormat::JsonObject),
            form: vec![],
        };
        let snap = LlmTaskExecutor::build_input_snapshot(
            "https://api.example.com",
            "gpt-4",
            "sys-prompt",
            "user-prompt",
            &config,
        );
        assert_eq!(snap["base_url"], "https://api.example.com");
        assert_eq!(snap["model"], "gpt-4");
        assert_eq!(snap["system_prompt"], "sys-prompt");
        assert_eq!(snap["user_prompt"], "user-prompt");
        assert_eq!(snap["api_key_ref"], "key-1");
        assert_eq!(snap["temperature"], 0.5);
        assert_eq!(snap["max_tokens"], 100);
    }

    #[test]
    fn test_try_build_success_output_text_format() {
        let resp = json!({
            "choices": [{"message": {"content": "Hello world"}, "finish_reason": "stop"}],
            "usage": {"total_tokens": 10},
            "model": "gpt-4"
        });
        let output = LlmTaskExecutor::try_build_success_output(&resp, "gpt-4", &None, 0)
            .unwrap();
        assert_eq!(output["content"], "Hello world");
        assert_eq!(output["finish_reason"], "stop");
        assert_eq!(output["model"], "gpt-4");
        assert_eq!(output["attempt"], 1);
    }

    #[test]
    fn test_try_build_success_output_json_object_valid() {
        let resp = json!({
            "choices": [{"message": {"content": "{\"key\":\"val\"}"}, "finish_reason": "stop"}],
            "usage": {"total_tokens": 5},
            "model": "gpt-4"
        });
        let output = LlmTaskExecutor::try_build_success_output(
            &resp,
            "gpt-4",
            &Some(LlmResponseFormat::JsonObject),
            0,
        )
        .unwrap();
        assert_eq!(output["content"], "{\"key\":\"val\"}");
        assert_eq!(output["parsed"]["key"], "val");
    }

    #[test]
    fn test_try_build_success_output_json_object_invalid() {
        let resp = json!({
            "choices": [{"message": {"content": "not json"}, "finish_reason": "stop"}],
            "usage": {"total_tokens": 2},
            "model": "gpt-4"
        });
        let err = LlmTaskExecutor::try_build_success_output(
            &resp,
            "gpt-4",
            &Some(LlmResponseFormat::JsonObject),
            0,
        );
        assert!(err.is_err());
        assert!(err.unwrap_err().contains("not valid JSON"));
    }

    #[test]
    fn test_try_build_success_output_missing_usage() {
        let resp = json!({
            "choices": [{"message": {"content": "ok"}, "finish_reason": "stop"}],
            "model": "gpt-4"
        });
        let output = LlmTaskExecutor::try_build_success_output(&resp, "gpt-4", &None, 1)
            .unwrap();
        assert_eq!(output["attempt"], 2);
        assert!(output["usage"].is_null());
    }
}
