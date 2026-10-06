# Customer Q&A: AWS Security Agent Penetration Testing

Date: 2026-09-29  
Reference: https://docs.aws.amazon.com/securityagent/latest/userguide/

---

## Q1: What do we need to prepare?

1. **Select a region** to host the Security Agent -- Singapore (ap-southeast-1) or Tokyo (ap-northeast-1). Both are the closest available regions to Hong Kong.
   - Note: Both regions use **global cross-region inference** -- inference requests may be processed in any commercial AWS region. Data remains stored in the originating region, and all cross-region data is encrypted in transit on the AWS network (not public internet). Review with your compliance team if data residency requirements apply.

2. **Select a non-production, production-like staging environment** as the test target.
   - AWS strongly recommends against testing in production.
   - Security Agent uses penetration testing tools from the Kali Linux distribution that may modify application state, data, or system configurations.
   - Staging environment should mirror production config but contain no live customer data and be isolated from production systems.
   - Reference: https://docs.aws.amazon.com/securityagent/latest/userguide/security-best-practices.html

3. **Network connectivity** -- if the target site is not publicly accessible:
   - Provide VPC ID, subnets (recommend multi-AZ for availability), and security groups.
   - Subnets require a NAT gateway for outbound connectivity.
   - Security Agent will provision elastic network interfaces (ENIs) into the specified subnets.
   - Use the PRIVATE_VPC domain verification method for internal endpoints.

4. **Create an Agent Space and verify domain ownership** for every target domain.
   - Verification options: DNS TXT record, HTTP route validation, or PRIVATE_VPC.
   - Sub-domains of a verified domain do not require individual verification (DNS TXT / HTTP route methods).
   - Testing cannot begin until domain ownership is verified.
   - Reference: https://docs.aws.amazon.com/securityagent/latest/userguide/enable-test-domain.html

5. **Set up credentials** if the target site requires authentication:
   - Store test credentials in AWS Secrets Manager, or provide a Lambda function as a dynamic credential vendor.
   - Create fresh credentials scoped specifically for the pentest -- do not reuse production credentials.
   - Reference: https://docs.aws.amazon.com/securityagent/latest/userguide/provide-testing-credentials.html

6. **(Optional but recommended) Provide application context** to improve test coverage and reduce false positives. Three methods:
   - **GitHub repositories** -- connect integrated repos (public or private); can specify branch.
   - **S3 bucket** -- configured at the Agent Space level, then select resources when creating the pentest.
   - **Direct upload** -- upload local files or paste plain text (API endpoint lists, URL patterns, instructions) in the pentest creation wizard.
   - Recommended materials: API documentation, OpenAPI/Swagger specs, architecture diagrams, configuration files, authentication flow diagrams, source code.
   - When source code is connected and "Enable automatic remediation" is checked, Security Agent can submit pull requests with code fixes directly to the repository.
   - Reference: https://docs.aws.amazon.com/securityagent/latest/userguide/perform-penetration-test.html (section "Attach additional resources")

7. **Testing duration and concurrency:**
   - Testing typically runs up to 16 hours depending on application breadth and configured risk categories.
   - You can set a maximum task-hours limit (minimum 20 hours). Task hours measure cumulative agent work time (parallel tasks counted), not wall-clock time. Task hours is also the billing unit.
   - Up to 5 concurrent test runs per account.

8. **Permissions required** -- the person setting up Agent Spaces and performing tests needs:
   - AWS account admin permissions (to create IAM roles for Security Agent). Default auto-created role is recommended.
   - Access to the Security Agent web application via IAM Identity Center.
   - DNS or HTTP access to add domain verification records.

---

## Q2: Do we need to inform AWS before testing?

**No.** AWS customers are allowed to conduct penetration testing on their own applications without prior AWS approval.

### Basis (from official AWS documentation)

**1. AWS Customer Support Policy for Penetration Testing** states:

> "AWS customers are welcome to carry out security assessments or penetration tests of their AWS infrastructure without prior approval for the services listed in the next section under 'Permitted Services.'"

The permitted services list explicitly includes Amazon Bedrock AgentCore (which Security Agent runs on), along with EC2, RDS, CloudFront, Aurora, API Gateway, Lambda, ECS, Fargate, OpenSearch, FSx, Transit Gateway, and Global Accelerator.

Reference: https://aws.amazon.com/security/penetration-testing/

**2. AWS Security Agent documentation** states:

> "Customers are responsible for ensuring they have proper authorization to test all systems that may be affected by their penetration testing activities. All use of AWS Security Agent must comply with the AWS Acceptable Use Policy."

Security Agent enforces domain ownership verification (DNS TXT, HTTP route, or PRIVATE_VPC) as the authorization control -- testing cannot begin until ownership is proven.

Reference: https://docs.aws.amazon.com/securityagent/latest/userguide/security-guidance.html

### Exceptions that DO require prior AWS approval

Submit a Simulated Events form (https://console.aws.amazon.com/support/contacts#/simulated-events) at least 2 weeks in advance for:

- Red/Blue/Purple team testing with Command and Control (C2)
- DDoS simulation testing
- Simulated phishing campaigns
- Malware testing

Standard penetration testing using AWS Security Agent against your own verified domains does NOT fall into these categories and does NOT require prior approval.

All use must comply with the AWS Acceptable Use Policy (https://aws.amazon.com/aup/).

---

## Additional Notes

- Security Agent detects OWASP Top 10 vulnerability categories including: SQL Injection, XSS, Command Injection, Code Injection, Privilege Escalation, IDOR, SSRF, Path Traversal, XXE, JWT Vulnerabilities, SSTI, Arbitrary File Upload.
- You can include/exclude specific risk categories per test.
- You can specify out-of-scope URLs to prevent testing against specific paths (e.g., destructive admin operations).
- Security Agent uses minimal-impact payloads by design (e.g., reads SQL version rather than dropping tables), but state changes can still occur.
- Findings are validated deterministically -- only high/medium confidence findings are reported by default.
- Each finding includes: impact analysis, reproducible attack path, and ready-to-implement code fix.
- Security Agent is not a replacement for professional penetration testing; integrate it into your security review workflow alongside human security professionals.
