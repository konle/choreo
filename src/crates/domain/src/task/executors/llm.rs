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

    fn is_success_status(status_code: u16) -> bool {
        (200..300).contains(&status_code)
    }

    fn is_rate_limited(status_code: u16) -> bool {
        status_code == 429
    }

    fn is_client_error(status_code: u16) -> bool {
        (400..500).contains(&status_code)
    }

    fn should_sleep_before_retry(attempt: u32, retry_count: u32, retry_delay: u32) -> bool {
        attempt < retry_count && retry_delay > 0
    }

    fn extract_llm_input_params<'a>(
        input: &'a serde_json::Value,
        config: &'a LlmTemplate,
    ) -> (&'a str, &'a str, &'a str, &'a str, &'a str) {
        let system_prompt = input.get("system_prompt").and_then(|v| v.as_str()).unwrap_or("");
        let user_prompt = input.get("user_prompt").and_then(|v| v.as_str()).unwrap_or(&config.user_prompt);
        let api_key = input.get("_api_key").and_then(|v| v.as_str()).unwrap_or("");
        let base_url = input.get("base_url").and_then(|v| v.as_str()).unwrap_or(&config.base_url);
        let model = input.get("model").and_then(|v| v.as_str()).unwrap_or(&config.model);
        (system_prompt, user_prompt, api_key, base_url, model)
    }

    async fn execute_llm_retry_loop(
        client: &Client,
        url: &str,
        body: &serde_json::Value,
        api_key: &str,
        model: &str,
        response_format: &Option<LlmResponseFormat>,
        base_url: &str,
        config: &LlmTemplate,
        input_snapshot: serde_json::Value,
    ) -> anyhow::Result<TaskExecutionResult> {
        let attempts = config.retry_count + 1;
        let mut last_error: Option<String> = None;

        for attempt in 0..attempts {
            let (status_code, resp_body) = match Self::send_llm_request(
                client, url, body, api_key, config.timeout,
            ).await {
                Ok(r) => r,
                Err(e) => {
                    last_error = Some(e);
                    if Self::should_sleep_before_retry(attempt, config.retry_count, config.retry_delay) {
                        tokio::time::sleep(std::time::Duration::from_secs(config.retry_delay as u64)).await;
                    }
                    continue;
                }
            };

            match Self::process_llm_response(
                status_code, &resp_body, model, response_format,
                attempt, attempts, config.retry_delay,
            ).await {
                Ok(Some(result)) => return Ok(TaskExecutionResult {
                    input: Some(input_snapshot),
                    ..result
                }),
                Err(e) => {
                    last_error = Some(e);
                    if Self::should_sleep_before_retry(attempt, config.retry_count, config.retry_delay) {
                        tokio::time::sleep(std::time::Duration::from_secs(config.retry_delay as u64)).await;
                    }
                }
                _ => {}
            }
        }

        let error_msg = last_error.unwrap_or_else(|| "Unknown error".to_string());
        Ok(TaskExecutionResult {
            status: NodeExecutionStatus::Failed,
            input: Some(input_snapshot),
            output: None,
            error_message: Some(error_msg),
        })
    }

    async fn send_llm_request(
        client: &Client,
        url: &str,
        body: &serde_json::Value,
        api_key: &str,
        timeout: u32,
    ) -> Result<(u16, String), String> {
        let request = Self::build_llm_request(client, url, body, api_key, timeout);
        let resp = request.send().await.map_err(|e| e.to_string())?;
        let status_code = resp.status().as_u16();
        let resp_body = resp.text().await.unwrap_or_default();
        Ok((status_code, resp_body))
    }

    async fn process_llm_response(
        status_code: u16,
        resp_body: &str,
        model: &str,
        response_format: &Option<LlmResponseFormat>,
        attempt: u32,
        total_attempts: u32,
        retry_delay: u32,
    ) -> Result<Option<TaskExecutionResult>, String> {
        if Self::is_success_status(status_code) {
            match serde_json::from_str::<serde_json::Value>(resp_body) {
                Ok(resp_json) => match Self::try_build_success_output(&resp_json, model, response_format, attempt) {
                    Ok(output) => return Ok(Some(TaskExecutionResult {
                        status: NodeExecutionStatus::Success,
                        input: None,
                        output: Some(output),
                        error_message: None,
                    })),
                    Err(e) => {
                        if Self::should_sleep_before_retry(attempt, total_attempts - 1, retry_delay) {
                            tokio::time::sleep(std::time::Duration::from_secs(retry_delay as u64)).await;
                        }
                        return Err(format!("JsonObject parse error: {}", e));
                    }
                },
                Err(e) => return Err(format!("response parse error: {}", e)),
            }
        }
        if Self::is_rate_limited(status_code) {
            return Err(format!("rate limited (429): {}", resp_body));
        }
        if Self::is_client_error(status_code) {
            return Ok(Some(TaskExecutionResult {
                status: NodeExecutionStatus::Failed,
                input: None,
                output: None,
                error_message: Some(format!("client error {}: {}", status_code, resp_body)),
            }));
        }
        Err(format!("server error {}: {}", status_code, resp_body))
    }

    fn build_llm_request(
        client: &Client,
        url: &str,
        body: &serde_json::Value,
        api_key: &str,
        timeout: u32,
    ) -> reqwest::RequestBuilder {
        let mut request = client
            .post(url)
            .header("Content-Type", "application/json")
            .json(body);

        if !api_key.is_empty() {
            request = request.header("Authorization", format!("Bearer {}", api_key));
        }
        if timeout > 0 {
            request = request.timeout(std::time::Duration::from_secs(timeout as u64));
        }
        request
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

        let (system_prompt, user_prompt, api_key, base_url, model) =
            Self::extract_llm_input_params(&input, config);

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

        Self::execute_llm_retry_loop(
            &self.client, &url, &body, api_key, model,
            &config.response_format, base_url, config,
            input_snapshot,
        )
        .await
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

    #[test]
    fn should_sleep_true() {
        assert!(LlmTaskExecutor::should_sleep_before_retry(0, 2, 5));
    }

    #[test]
    fn should_sleep_false_at_limit() {
        assert!(!LlmTaskExecutor::should_sleep_before_retry(2, 2, 5));
    }

    #[test]
    fn should_sleep_false_zero_delay() {
        assert!(!LlmTaskExecutor::should_sleep_before_retry(0, 2, 0));
    }

    #[tokio::test]
    async fn process_llm_success() {
        let result = LlmTaskExecutor::process_llm_response(
            200, r#"{"choices":[{"message":{"content":"hi"},"finish_reason":"stop"}]}"#,
            "gpt-4", &None, 0, 1, 0,
        ).await;
        assert!(result.is_ok());
        let r = result.unwrap();
        assert!(r.is_some());
        assert_eq!(r.unwrap().status, NodeExecutionStatus::Success);
    }

    #[tokio::test]
    async fn process_llm_rate_limited() {
        let result = LlmTaskExecutor::process_llm_response(
            429, "rate limited", "gpt-4", &None, 0, 1, 0,
        ).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn process_llm_client_error_fails() {
        let result = LlmTaskExecutor::process_llm_response(
            400, "bad request", "gpt-4", &None, 0, 1, 0,
        ).await;
        assert!(result.is_ok());
        let r = result.unwrap().unwrap();
        assert_eq!(r.status, NodeExecutionStatus::Failed);
    }

    #[tokio::test]
    async fn process_llm_invalid_json() {
        let result = LlmTaskExecutor::process_llm_response(
            200, "not json", "gpt-4", &None, 0, 1, 0,
        ).await;
        assert!(result.is_err());
    }
}
