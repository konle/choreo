use async_trait::async_trait;
use chrono::Utc;
use reqwest::Client;
use rhai::Scope;
use serde_json::json;
use tracing::{error, warn};

use crate::plugin::rhai_engine;
use crate::shared::workflow::TaskType;
use crate::task::entity::task_definition::{HttpMethod, TaskInstanceEntity, TaskTemplate};
use crate::task::http_template_resolve::effective_http_request;
use crate::task::interface::{TaskExecutionResult, TaskExecutor};
use crate::workflow::entity::workflow_definition::NodeExecutionStatus;

pub struct HttpTaskExecutor {
    pub client: Client,
}

struct HttpResponse {
    status_code: u16,
    body: String,
    duration_ms: u64,
}

struct HttpRequestConfig<'a> {
    url: &'a str,
    method: &'a HttpMethod,
    headers_obj: &'a serde_json::Map<String, serde_json::Value>,
    body_json: &'a Option<serde_json::Value>,
    timeout: u32,
    retry_count: u32,
    retry_delay: u32,
    success_condition: &'a Option<String>,
}

impl Default for HttpTaskExecutor {
    fn default() -> Self {
        Self { client: Client::new() }
    }
}

impl HttpTaskExecutor {
    pub fn new() -> Self {
        Self::default()
    }

    fn is_http_success(status_code: u16) -> bool { (200..300).contains(&status_code) }
    fn should_retry(attempt: u32, retry_count: u32) -> bool { attempt < retry_count }

    fn build_request(
        client: &Client,
        url: &str,
        method: &HttpMethod,
        headers_obj: &serde_json::Map<String, serde_json::Value>,
        body_json: &Option<serde_json::Value>,
        timeout_secs: u32,
    ) -> reqwest::RequestBuilder {
        let mut request = match method {
            HttpMethod::Get => client.get(url),
            HttpMethod::Post => client.post(url),
            HttpMethod::Put => client.put(url),
            HttpMethod::Delete => client.delete(url),
            HttpMethod::Head => client.head(url),
        };

        for (hk, hv) in headers_obj {
            let s = match hv {
                serde_json::Value::String(s) => s.clone(),
                serde_json::Value::Null => continue,
                other => other.to_string(),
            };
            request = request.header(hk.as_str(), s.as_str());
        }

        if let Some(bj) = body_json && !bj.is_null() && bj != &serde_json::Value::Object(serde_json::Map::new()) {
            request = request.json(bj);
        }

        if timeout_secs > 0 {
            request = request.timeout(std::time::Duration::from_secs(timeout_secs as u64));
        }

        request
    }

    async fn send_request(
        &self,
        url: &str,
        method: &HttpMethod,
        headers_obj: &serde_json::Map<String, serde_json::Value>,
        body_json: &Option<serde_json::Value>,
        timeout_secs: u32,
    ) -> Result<HttpResponse, String> {
        let request = Self::build_request(&self.client, url, method, headers_obj, body_json, timeout_secs);

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

    fn evaluate_condition(body_val: &serde_json::Value, condition: &str) -> Result<bool, String> {
        let engine = rhai_engine::create_engine();
        let mut scope = Scope::new();
        rhai_engine::inject_context_flat(&mut scope, body_val);
        engine
            .eval_with_scope::<bool>(&mut scope, condition)
            .map_err(|e| e.to_string())
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

        match Self::evaluate_condition(&body_val, condition) {
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

    fn evaluate_http_attempt(
        &self,
        url: &str,
        resp: &HttpResponse,
        success_condition: &Option<String>,
        task_id: &str,
        attempt: u32,
        input_snapshot: &serde_json::Value,
    ) -> Result<TaskExecutionResult, (String, Option<serde_json::Value>)> {
        if !Self::is_http_success(resp.status_code) {
            warn!(task_instance_id = %task_id, url = %url, status_code = resp.status_code, attempt = attempt + 1, "HTTP task returned non-2xx status");
            return Err((format!("HTTP {}: {}", resp.status_code, resp.body), None));
        }
        if let Some(condition) = success_condition {
            let passed = self.evaluate_success_condition(task_id, &resp.body, condition);
            let output = Self::build_response_output(resp.status_code, &resp.body, resp.duration_ms, attempt, Some((condition, passed)));
            if passed {
                return Ok(TaskExecutionResult { status: NodeExecutionStatus::Success, input: Some(input_snapshot.clone()), output: Some(output), error_message: None });
            }
            return Err((format!("success_condition `{}` not met", condition), Some(output)));
        }
        let output = Self::build_response_output(resp.status_code, &resp.body, resp.duration_ms, attempt, None);
        Ok(TaskExecutionResult { status: NodeExecutionStatus::Success, input: Some(input_snapshot.clone()), output: Some(output), error_message: None })
    }

    async fn retry_sleep_if_needed(attempt: u32, retry_count: u32, retry_delay: u32) {
        if Self::should_retry(attempt, retry_count) && retry_delay > 0 {
            tokio::time::sleep(std::time::Duration::from_secs(retry_delay as u64)).await;
        }
    }

    async fn execute_single_http_attempt(
        &self,
        config: &HttpRequestConfig<'_>,
        attempt: u32,
        task_id: &str,
        input_snapshot: &serde_json::Value,
    ) -> (Option<TaskExecutionResult>, Option<String>, Option<serde_json::Value>) {
        let resp = match self.send_request(config.url, config.method, config.headers_obj, config.body_json, config.timeout).await {
            Ok(r) => r,
            Err(e) => {
                warn!(task_instance_id = %task_id, url = %config.url, attempt = attempt + 1, error = %e, "HTTP request failed");
                Self::retry_sleep_if_needed(attempt, config.retry_count, config.retry_delay).await;
                return (None, Some(e), None);
            }
        };

        match self.evaluate_http_attempt(config.url, &resp, config.success_condition, task_id, attempt, input_snapshot) {
            Ok(result) => (Some(result), None, None),
            Err((err_msg, out)) => {
                Self::retry_sleep_if_needed(attempt, config.retry_count, config.retry_delay).await;
                (None, Some(err_msg), out)
            }
        }
    }

    async fn execute_http_retry_loop(
        &self,
        config: HttpRequestConfig<'_>,
        task_id: &str,
        input_snapshot: serde_json::Value,
    ) -> anyhow::Result<TaskExecutionResult> {
        let mut last_error: Option<String> = None;
        let mut last_output: Option<serde_json::Value> = None;
        let attempts = config.retry_count + 1;

        for attempt in 0..attempts {
            let (result, err, out) = Self::execute_single_http_attempt(
                self, &config, attempt, task_id, &input_snapshot,
            ).await;
            if let Some(result) = result {
                return Ok(result);
            }
            last_error = err;
            last_output = out;
        }

        let error_msg = last_error.unwrap_or_else(|| "Unknown error".to_string());
        Ok(TaskExecutionResult { status: NodeExecutionStatus::Failed, input: Some(input_snapshot), output: last_output, error_message: Some(error_msg) })
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
        let http_config = HttpRequestConfig {
            url: &url,
            method: &method,
            headers_obj: &headers_obj,
            body_json: &body_json,
            timeout: config.timeout,
            retry_count: config.retry_count,
            retry_delay: config.retry_delay,
            success_condition: &config.success_condition,
        };
        self.execute_http_retry_loop(http_config, &task_instance.task_instance_id, input_snapshot).await
    }

    fn task_type(&self) -> TaskType {
        TaskType::Http
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evaluate_condition_true() {
        let body = serde_json::json!({"status": "ok"});
        assert!(HttpTaskExecutor::evaluate_condition(&body, "status == \"ok\"").unwrap());
    }

    #[test]
    fn evaluate_condition_false() {
        let body = serde_json::json!({"status": "err"});
        assert!(!HttpTaskExecutor::evaluate_condition(&body, "status == \"ok\"").unwrap());
    }

    #[test]
    fn evaluate_condition_error() {
        let body = serde_json::json!({});
        assert!(HttpTaskExecutor::evaluate_condition(&body, "unknown_var > 0").is_err());
    }

    #[test]
    fn build_request_methods_and_url() {
        let client = reqwest::Client::new();
        let headers = serde_json::Map::new();
        let req = HttpTaskExecutor::build_request(
            &client, "http://example.com", &HttpMethod::Get, &headers, &None, 0,
        );
        // reqwest's RequestBuilder doesn't expose method directly, but we check it builds without panicking
        let req = HttpTaskExecutor::build_request(
            &client, "http://example.com", &HttpMethod::Post, &headers, &None, 10,
        );
        let _req = req; // just verify it doesn't panic
    }
}
