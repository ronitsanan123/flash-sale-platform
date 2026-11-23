# Illustrative AWS Terraform. Not applied by this project. Pin provider versions,
# add org policy/tags/state backend, and complete security review before use.
terraform {
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.0" }
  }
}

variable "name" { type = string, default = "flashsale" }
variable "origin_domain" { type = string }
variable "queue_signing_secret_arn" { type = string }

resource "aws_cloudfront_origin_access_control" "edge" {
  name                              = "${var.name}-edge-oac"
  description                       = "Origin access for flash-sale edge"
  origin_access_control_origin_type = "custom"
  signing_behavior                  = "always"
  signing_protocol                  = "sigv4"
}

resource "aws_cloudfront_distribution" "edge" {
  enabled         = true
  comment         = "${var.name} CDN; waiting-room logic is supplied by an approved edge function/vendor"
  price_class     = "PriceClass_All"
  is_ipv6_enabled = true

  origin {
    domain_name = var.origin_domain
    origin_id   = "flashsale-api-origin"
    custom_origin_config {
      http_port              = 80
      https_port             = 443
      origin_protocol_policy = "https-only"
      origin_ssl_protocols   = ["TLSv1.2"]
    }
  }

  default_cache_behavior {
    target_origin_id       = "flashsale-api-origin"
    viewer_protocol_policy = "redirect-to-https"
    allowed_methods        = ["GET", "HEAD", "OPTIONS", "POST"]
    cached_methods         = ["GET", "HEAD"]
    compress               = true
    forwarded_values {
      query_string = true
      headers      = ["Authorization", "X-User-ID", "X-Item-ID"]
      cookies { forward = "all" }
    }
    min_ttl     = 0
    default_ttl = 0
    max_ttl     = 1
  }

  restrictions {
    geo_restriction { restriction_type = "none" }
  }
  viewer_certificate { cloudfront_default_certificate = true }
}

output "distribution_domain_name" { value = aws_cloudfront_distribution.edge.domain_name }
output "queue_signing_secret_arn_assumption" { value = var.queue_signing_secret_arn }
