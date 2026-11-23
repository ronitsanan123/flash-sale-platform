# Illustrative AWS WAF rate rule. This is a coarse IP control, not the full
# token/account/item admission controller. Tune thresholds to traffic shape.
variable "web_acl_name" { type = string, default = "flashsale-edge" }
variable "rate_limit_per_5m_per_ip" { type = number, default = 2000 }

resource "aws_wafv2_web_acl" "edge" {
  name        = var.web_acl_name
  description = "Baseline abuse protection; queue admission is enforced separately"
  scope       = "CLOUDFRONT"
  provider    = aws.us_east_1

  default_action { allow {} }
  rule {
    name     = "per-ip-rate-limit"
    priority = 10
    action { block {} }
    statement {
      rate_based_statement {
        limit              = var.rate_limit_per_5m_per_ip
        aggregate_key_type = "IP"
      }
    }
    visibility_config {
      cloudwatch_metrics_enabled = true
      metric_name                = "flashsale-ip-rate-limit"
      sampled_requests_enabled   = true
    }
  }
  visibility_config {
    cloudwatch_metrics_enabled = true
    metric_name                = "flashsale-edge-waf"
    sampled_requests_enabled   = true
  }
}
