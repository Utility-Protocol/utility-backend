/// GraphQL types for meter readings and billing event subscriptions (#242).
use async_graphql::*;
use serde::{Deserialize, Serialize};

/// A single meter reading event delivered via subscription.
#[derive(SimpleObject, Clone, Debug, Serialize, Deserialize)]
pub struct MeterReading {
    /// Unique reading identifier.
    pub reading_id: String,
    /// The meter device ID.
    pub device_id: String,
    /// Service type (electricity, water, gas).
    pub service_type: String,
    /// Reading value.
    pub value: String,
    /// Unit of measurement (kWh, L, m³).
    pub unit: String,
    /// ISO-8601 timestamp of the reading.
    pub timestamp: String,
}

/// A billing event delivered via subscription.
#[derive(SimpleObject, Clone, Debug, Serialize, Deserialize)]
pub struct BillingEvent {
    /// Unique event identifier.
    pub event_id: String,
    /// The meter device ID.
    pub device_id: String,
    /// Service type (electricity, water, gas).
    pub service_type: String,
    /// Billed amount.
    pub amount: String,
    /// Currency code (USD, EUR, etc.).
    pub currency: String,
    /// Billing status (pending, billed, paid, overdue).
    pub status: String,
    /// ISO-8601 timestamp of the billing event.
    pub timestamp: String,
    /// Human-readable description.
    pub description: Option<String>,
}

/// Filter input for `meterReadings` subscription.
#[derive(InputObject, Debug)]
pub struct MeterReadingFilter {
    /// Only deliver readings for this device.
    pub device_id: Option<String>,
    /// Only deliver readings for this service type.
    pub service_type: Option<String>,
}

/// Filter input for `billingEvents` subscription.
#[derive(InputObject, Debug)]
pub struct BillingEventFilter {
    /// Only deliver events for this device.
    pub device_id: Option<String>,
    /// Only deliver events for this service type.
    pub service_type: Option<String>,
}