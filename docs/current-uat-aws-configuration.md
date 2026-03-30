# Current UAT AWS Configuration

## Purpose

This document records the final deployed Phase 1 UAT AWS runtime, the current live endpoints, the major design decisions behind the deployment, and the remaining hardening steps before production.

## 1. Public Endpoints

- Web application:
  - `https://d22xc0mfhkv9bk.cloudfront.net`
- Public health endpoint:
  - `https://d22xc0mfhkv9bk.cloudfront.net/health`
- Public API path prefix through CloudFront:
  - `https://d22xc0mfhkv9bk.cloudfront.net/api/v1`

## 2. Internal Runtime Endpoints

- CloudFront distribution:
  - `E17SODID532HTA`
- Interactive API origin load balancer:
  - `sales-plan-api-alb-1826309259.ap-southeast-5.elb.amazonaws.com`
- ECS cluster:
  - `sales-planning-demo-cluster`
- ECS service:
  - `sales-planning-demo-api`
- Active PostgreSQL endpoint:
  - `sales-planning-demo-pg-private.c78cmc6im0o5.ap-southeast-5.rds.amazonaws.com:5432`

## 3. Current AWS Stack Components

### 3.1 Edge and static web

- `Amazon S3` static website bucket for the built web bundle
- `Amazon CloudFront` for global web delivery
- `AWS WAF` attached to CloudFront for baseline protection

### 3.2 Interactive API

- `Amazon ECS Fargate`
- `1` running task in UAT
- application image hosted in `Amazon ECR`
- async job orchestration hosted in the API service
- async job state, progress, payloads, retention, and downloadable outputs persisted in PostgreSQL
- ALB listener with:
  - default `403`
  - forward only when the CloudFront origin header is present

### 3.3 Database

- `Amazon RDS for PostgreSQL`
- active DB instance:
  - `sales-planning-demo-pg-private`
- instance class:
  - `db.t3.micro`
- storage:
  - `20 GB gp2`
- public access:
  - disabled
- active subnet group:
  - `sales-planning-demo-rds-private-subnets`

### 3.4 Security and secrets

- Microsoft Entra-backed frontend sign-in
- backend API authorization enabled
- authoritative DB credentials stored in `AWS Secrets Manager`
- live ECS startup uses direct PostgreSQL username/password environment values injected by the CloudFormation deployment to avoid runtime secret-resolution stalls
- ALB ingress restricted to the AWS-managed CloudFront origin-facing prefix list
- ASP.NET Data Protection keys persisted in PostgreSQL instead of container-local storage

## 4. Network Placement

### 4.1 Current placement

- VPC:
  - `vpc-0c29c4e611198869c`
- ECS tasks:
  - still in the existing public subnets for UAT simplicity
- Active RDS:
  - now in true private subnets created specifically for the database

### 4.2 Private RDS subnets

- `subnet-0c96682e9fc725ec7` in `ap-southeast-5a`
- `subnet-063a1fd7d230e9d50` in `ap-southeast-5b`
- `subnet-067a46b989e112477` in `ap-southeast-5c`
- DB subnet group:
  - `sales-planning-demo-rds-private-subnets`

## 5. Key Design Decisions

### 5.1 Why ECS Fargate was chosen for the interactive API

- removes Lambda cold-start pressure from interactive planning
- avoids Lambda payload and timeout constraints on grid traffic
- provides steadier response times for planning, undo/redo, and master-data maintenance

### 5.2 Why RDS PostgreSQL is the authoritative store

- supports larger planning volumes than the previous SQLite model
- enables native relational filtering and projection
- supports the Phase 2 merchandising data foundation without redesign

### 5.3 Why the database move used replacement rather than in-place subnet migration

- AWS blocked the in-place subnet-group change for the existing instance
- the safe cutover path was:
  - stop interactive writes
  - take a manual snapshot
  - restore a replacement instance in the private subnet group
  - switch ECS to the new endpoint

### 5.4 Why the ALB remains HTTP from CloudFront for now

- there is not yet a Route 53 hosted zone and ACM certificate for the ALB region
- ALB HTTPS origin completion is therefore deferred intentionally

## 6. Live Security Posture

- CloudFront serves the public app over HTTPS
- WAF is attached to CloudFront
- direct public ALB access is blocked by:
  - security group source restriction
  - secret origin header validation
- database is no longer publicly accessible
- API returns `401` on protected routes without a valid token
- Data Protection keys survive ECS task replacement because they are stored in PostgreSQL

## 7. Cost-Aware UAT Decisions

- one active ECS task
- one active PostgreSQL instance
- one parked rollback DB only during transition windows
- no Redis yet
- no Route 53 custom domain yet
- no ALB certificate yet

## 8. Temporary Rollback State

- previous DB instance:
  - `sales-planning-demo-pg`
- current state:
  - stopped or stopping as a temporary rollback copy

Important note:

- a stopped RDS instance can automatically restart after about `7` days
- once UAT acceptance is complete, the rollback instance should be snapshotted and deleted to avoid unnecessary cost

## 9. Remaining Hardening Steps

- add Route 53 hosted zone and ACM certificate
- move CloudFront to HTTPS-only origin traffic to the ALB
- consider moving ECS tasks into private subnets with the required NAT or endpoint strategy
- consider replacing deployment-time DB credential injection with a private Secrets Manager or SSM retrieval path once the required endpoint strategy is available
- add richer operational observability and alarms around job backlog, reconciliation failures, and edit latency
- remove the stopped rollback DB when it is no longer needed
