use async_trait::async_trait;
use reqwest::Client;
use serde_json::json;
use tracing::{debug, error, warn};

use crate::shared::workflow::TaskType;
use crate::task::entity::task_definition::{LlmResponseFormat, TaskInstanceEntity, TaskTemplate};
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

        let mut messages = vec![];
        if !system_prompt.is_empty() {
            messages.push(json!({"role": "system", "content": system_prompt}));
        }
        messages.push(json!({"role": "user", "content": user_prompt}));

        let mut body = json!({
            "model": model,
            "messages": messages,
        });

        if let Some(temp) = config.temperature {
            body["temperature"] = json!(temp);
        }
        if let Some(max_tok) = config.max_tokens {
            body["max_tokens"] = json!(max_tok);
        }
        if let Some(LlmResponseFormat::JsonObject) = &config.response_format {
            body["response_format"] = json!({"type": "json_object"});
        }

        let input_snapshot = json!({
            "base_url": base_url,
            "model": model,
            "system_prompt": system_prompt,
            "user_prompt": user_prompt,
            "api_key_ref": config.api_key_ref,
            "temperature": config.temperature,
            "max_tokens": config.max_tokens,
            "response_format": config.response_format,
        });

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

                                let parsed = if matches!(
                                    config.response_format,
                                    Some(LlmResponseFormat::JsonObject)
                                ) {
                                    match serde_json::from_str::<serde_json::Value>(&content) {
                                        Ok(v) => Some(v),
                                        Err(e) => {
                                            warn!(
                                                task_instance_id = %task_instance.task_instance_id,
                                                attempt = attempt + 1,
                                                error = %e,
                                                "LLM returned non-JSON content with JsonObject format"
                                            );
                                            last_error = Some(format!(
                                                "response_format=JsonObject but content is not valid JSON: {}",
                                                e
                                            ));
                                            if attempt < config.retry_count {
                                                if config.retry_delay > 0 {
                                                    tokio::time::sleep(
                                                        std::time::Duration::from_secs(
                                                            config.retry_delay as u64,
                                                        ),
                                                    )
                                                    .await;
                                                }
                                            }
                                            continue;
                                        }
                                    }
                                } else {
                                    None
                                };

                                let mut output = json!({
                                    "content": content,
                                    "usage": usage,
                                    "model": resp_model,
                                    "finish_reason": finish_reason,
                                    "attempt": attempt + 1,
                                });
                                if let Some(p) = parsed {
                                    output["parsed"] = p;
                                }

                                debug!(
                                    task_instance_id = %task_instance.task_instance_id,
                                    model = %resp_model,
                                    finish_reason = %finish_reason,
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
