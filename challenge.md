The Challenge: Cloud Instance Health Monitor & Alerting Dispatcher

Time Limit: 3 Hours
Objective: Design, build, and deploy a service that continuously monitors the health of user-registered HTTP endpoints (simulating cloud instances) and dispatches webhook alerts when an endpoint goes offline.

Core Requirements:

Target Management API: Build REST endpoints to POST a new URL to monitor, GET the current status of all monitored URLs, and DELETE a target.

Background Polling Engine: Implement an asynchronous worker or scheduled task that pings all registered URLs every 10 to 30 seconds.

State Management & Resilience: Store the endpoints and their current status (UP, DOWN, PENDING) in a persistent datastore. The polling engine must not crash if a single endpoint times out or returns a malformed response.

Alert Dispatcher: If a monitored URL returns a non-200 status code for two consecutive checks, transition its state to DOWN and fire a simulated HTTP POST request to a user-defined webhook URL.

Deployment: Deploy the API and the background worker live to a production environment (such as DigitalOcean App Platform or a Droplet) so the system can be actively tested during the review.

Permitted Tools: AI coding assistants (Cursor, Claude Code, GitHub Copilot) are explicitly permitted and expected.