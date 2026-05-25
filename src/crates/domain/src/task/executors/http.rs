use async_trait::async_trait;
use chrono::Utc;
use reqwest::Client;
use rhai::Scope;
use serde_json::json;
use tracing::{debug, error, warn};

use crate::plugin::rhai_engine;
use crate::shared::workflow::TaskType;
use crate::task::entity::task_definition::{HttpMethod, TaskInstanceEntity, TaskTemplate};
use crate::task::http_template_resolve::effective_http_request;
use crate::task::interface::{TaskExecutionResult, TaskExecutor};
use crate::workflow::entity::workflow_definition::NodeExecutionStatus;

pub struct HttpTaskExecutor {
    client: Client,
}

struct HttpResponse {
    status_code: u16,
    body: String,
    duration_ms: u64,
}

impl HttpTaskExecutor {
    pub fn new() -> Self {
        Self {
            client: Client::new(),
        }
    }

    async fn send_request(
        &self,
        url: &str,
        method: &HttpMethod,
        headers_obj: &serde_json::Map<String, serde_json::Value>,
        body_json: &Option<serde_json::Value>,
        timeout_secs: u32,
    ) -> Result<HttpResponse, String> {
        let mut request = match method {
            HttpMethod::Get => self.client.get(url),
            HttpMethod::Post => self.client.post(url),
            HttpMethod::Put => self.client.put(url),
            HttpMethod::Delete => self.client.delete(url),
            HttpMethod::Head => self.client.head(url),
        };

        for (hk, hv) in headers_obj {
            let s = match hv {
                serde_json::Value::String(s) => s.clone(),
                serde_json::Value::Null => continue,
                other => other.to_string(),
            };
            request = request.header(hk.as_str(), s.as_str());
        }

        if let Some(bj) = body_json {
            if !bj.is_null() && bj != &serde_json::Value::Object(serde_json::Map::new()) {
                request = request.json(bj);
            }
        }

        if timeout_secs > 0 {
            request = request.timeout(std::time::Duration::from_secs(timeout_secs as u64));
        }

        let start = Utc::now();
        let resp = request.send().await.map_err(|e| e.to_string())?;
        let status_code = resp.status().as_u16();
        let body = resp.text().await.unwrap_or_default();
        let duration_ms = (Utc::now() - start).num_milliseconds().max(0) as u64;

        Ok(HttpResponse {
            status_code,
            body,
            duration_ms,
        })
    }

    fn evaluate_success_condition(
        &self,
        task_instance_id: &str,
        resp_body: &str,
        condition: &str,
    ) -> bool {
        let body_val = match serde_json::from_str::<serde_json::Value>(resp_body) {
            Ok(v) => v,
            Err(_) => {
                warn!(
                    task_instance_id = %task_instance_id,
                    "response body is not valid JSON, success_condition cannot be evaluated"
                );
                return false;
            }
        };

        let engine = rhai_engine::create_engine();
        let mut scope = Scope::new();
        rhai_engine::inject_context_flat(&mut scope, &body_val);
        match engine.eval_with_scope::<bool>(&mut scope, condition) {
            Ok(v) => v,
            Err(e) => {
                warn!(
                    task_instance_id = %task_instance_id,
                    condition = %condition,
                    error = %e,
                    "success_condition eval error, treating as not met"
                );
                false
            }
        }
    }

    fn build_response_output(
        status_code: u16,
        resp_body: &str,
        duration_ms: u64,
        attempt: u32,
        condition: Option<(&str, bool)>,
    ) -> serde_json::Value {
        let body_val = serde_json::from_str::<serde_json::Value>(resp_body)
            .unwrap_or(serde_json::Value::String(resp_body.to_string()));
        let mut output = json!({
            "status_code": status_code,
            "body": body_val,
            "duration_ms": duration_ms,
            "attempt": attempt + 1,
        });
        if let Some((expr, result)) = condition {
            output["condition_result"] = json!(result);
            output["condition_expression"] = json!(expr);
        }
        output
    }
}

#[async_trait]
impl TaskExecutor for HttpTaskExecutor {
    async fn execute_task(
        &self,
        task_instance: &TaskInstanceEntity,
    ) -> anyhow::Result<TaskExecutionResult> {
        let config = match &task_instance.task_template {
            TaskTemplate::Http(c) => c,
            other => {
                error!(task_instance_id = %task_instance.task_instance_id, template = ?other, "expected Http config");
                return Err(anyhow::anyhow!("Expected Http config but got other"));
            }
        };

        let empty_ctx = json!({});
        let (input_snapshot, url, method, headers_obj, body_json) =
            effective_http_request(task_instance, config, &empty_ctx);

        if url.is_empty() {
            return Err(anyhow::anyhow!("HTTP task has empty url after resolution"));
        }

        let mut last_error: Option<String> = None;
        let mut last_output: Option<serde_json::Value> = None;
        let attempts = config.retry_count + 1;
        let tid = task_instance.task_instance_id.as_str();

        for attempt in 0..attempts {
            let resp = match self
                .send_request(&url, &method, &headers_obj, &body_json, config.timeout)
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    warn!(task_instance_id = %tid, url = %url, attempt = attempt + 1, error = %e, "HTTP request failed");
                    last_error = Some(e);
                    if attempt < config.retry_count {
                        let delay = config.retry_delay as u64;
                        if delay > 0 {
                            tokio::time::sleep(std::time::Duration::from_secs(delay)).await;
                        }
                    }
                    continue;
                }
            };

            if !(200..300).contains(&resp.status_code) {
                warn!(task_instance_id = %tid, url = %url, status_code = resp.status_code, attempt = attempt + 1, "HTTP task returned non-2xx status");
                last_error = Some(format!("HTTP {}: {}", resp.status_code, resp.body));
            } else if let Some(ref condition) = config.success_condition {
                let passed = self.evaluate_success_condition(tid, &resp.body, condition);
                let output = Self::build_response_output(
                    resp.status_code,
                    &resp.body,
                    resp.duration_ms,
                    attempt,
                    Some((condition, passed)),
                );

                if passed {
                    debug!(task_instance_id = %tid, url = %url, status_code = resp.status_code, condition = %condition, duration_ms = resp.duration_ms, "HTTP request succeeded (condition met)");
                    return Ok(TaskExecutionResult {
                        status: NodeExecutionStatus::Success,
                        input: Some(input_snapshot),
                        output: Some(output),
                        error_message: None,
                    });
                }

                warn!(task_instance_id = %tid, url = %url, condition = %condition, attempt = attempt + 1, "success_condition not met");
                last_error = Some(format!(
                    "success_condition `{}` not met (body: {})",
                    condition, resp.body
                ));
                last_output = Some(output);
            } else {
                debug!(task_instance_id = %tid, url = %url, status_code = resp.status_code, duration_ms = resp.duration_ms, "HTTP request succeeded");
                let output = Self::build_response_output(
                    resp.status_code,
                    &resp.body,
                    resp.duration_ms,
                    attempt,
                    None,
                );
                return Ok(TaskExecutionResult {
                    status: NodeExecutionStatus::Success,
                    input: Some(input_snapshot),
                    output: Some(output),
                    error_message: None,
                });
            }

            if attempt < config.retry_count {
                let delay = config.retry_delay as u64;
                if delay > 0 {
                    tokio::time::sleep(std::time::Duration::from_secs(delay)).await;
                }
            }
        }

        let error_msg = last_error.unwrap_or_else(|| "Unknown error".to_string());
        error!(task_instance_id = %tid, url = %url, attempts = attempts, error = %error_msg, "HTTP task failed after all retries");

        Ok(TaskExecutionResult {
            status: NodeExecutionStatus::Failed,
            input: Some(input_snapshot),
            output: last_output,
            error_message: Some(error_msg),
        })
    }

    fn task_type(&self) -> TaskType {
        TaskType::Http
    }
}
