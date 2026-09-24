# Kafka Saga Microservices

A Spring Boot microservices project demonstrating the **Saga Pattern with Apache Kafka** for distributed transaction management.

## Services

* **Order Service** – Creates and manages orders.
* **Product Service** – Manages products and inventory reservation/release.
* **Payment Service** – Processes payments and handles payment failures.
* **Core Service** – Acts as the Saga orchestrator and coordinates commands/events.
* **Credit Card Processor Service** – Simulates an external payment processor using WebClient.

## Saga Flow

```text
Order Created
     ↓
Reserve Product
     ↓
Product Reserved
     ↓
Process Payment
     ↓
Payment Successful
     ↓
Order Completed
```

### Compensation Flow

```text
Payment Failed
     ↓
Release Product
     ↓
Product Inventory Restored
     ↓
Order Cancelled
```

## Technologies

* Java
* Spring Boot
* Spring Kafka
* Apache Kafka
* Spring WebClient
* Spring Data JPA / Hibernate
* Maven

## Key Concepts

* Event-driven microservices
* Kafka commands and events
* Saga orchestration
* Compensation transactions
* Kafka retry and DLT
* WebClient-based service communication
* Distributed transaction handling
