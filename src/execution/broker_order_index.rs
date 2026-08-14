use std::{
    collections::{HashMap, HashSet},
    ops::Deref,
};

use nautilus_model::enums::{OrderType, TimeInForce};
use rust_decimal::Decimal;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) enum TbankBrokerOrderRoute {
    RegularOrder,
    StopOrder,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct TbankBrokerOrderIdentity {
    pub(super) route: TbankBrokerOrderRoute,
    pub(super) broker_order_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct TbankManagedOrderContext {
    pub(super) side: Option<crate::common::TbankOrderSide>,
    pub(super) order_type: Option<crate::common::TbankOrderType>,
    /// Canonical Nautilus type used for reports reconstructed from broker stop state.
    ///
    /// T-Bank can return external stop subtypes (for example a regular take-profit
    /// with an exchange limit child) which are not representable by the submit-side
    /// `TbankOrderType` enum. Keep that reporting identity separately.
    pub(super) report_order_type: Option<OrderType>,
    pub(super) time_in_force: Option<TimeInForce>,
    pub(super) quantity_units: Option<Decimal>,
    pub(super) trailing: Option<crate::execution::TbankTrailingStopParams>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum TbankCancelTarget {
    Ready(TbankBrokerOrderIdentity),
    Pending {
        route: TbankBrokerOrderRoute,
        client_order_id: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct TbankResolvedStreamOrderIdentity {
    pub(super) venue_order_id: String,
    pub(super) pending_cancel: Option<TbankBrokerOrderIdentity>,
}

impl Deref for TbankResolvedStreamOrderIdentity {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.venue_order_id.as_str()
    }
}

#[derive(Debug, Default)]
pub(super) struct TbankBrokerOrderIndex {
    by_client_order_id: HashMap<String, TbankBrokerOrderIdentity>,
    pending_route_by_client_order_id: HashMap<String, TbankBrokerOrderRoute>,
    broker_request_id_by_client_order_id: HashMap<String, String>,
    client_order_id_by_request_id: HashMap<String, String>,
    by_venue_order_id: HashMap<String, TbankBrokerOrderRoute>,
    stop_broker_order_ids: HashSet<String>,
    client_order_id_by_venue_order_id: HashMap<String, String>,
    canonical_venue_order_id_by_alias: HashMap<String, String>,
    managed_context_by_client_order_id: HashMap<String, TbankManagedOrderContext>,
    pending_cancel_client_order_ids: HashSet<String>,
}

const TBANK_BROKER_REQUEST_NAMESPACE: Uuid =
    Uuid::from_u128(0x6f55_8adb_586d_51bc_8c50_4f68_b3b5_b0d1);

/// Derives a deterministic T-Bank request ID from a Nautilus client order ID.
pub fn tbank_broker_request_id_for_client_order_id(client_order_id: &str) -> String {
    Uuid::new_v5(&TBANK_BROKER_REQUEST_NAMESPACE, client_order_id.as_bytes()).to_string()
}

impl TbankBrokerOrderIndex {
    pub(super) fn record_client_order_route(
        &mut self,
        route: TbankBrokerOrderRoute,
        client_order_id: &str,
    ) {
        if client_order_id.is_empty() {
            return;
        }
        if self.by_client_order_id.contains_key(client_order_id) {
            return;
        }
        self.pending_route_by_client_order_id
            .entry(client_order_id.to_string())
            .or_insert(route);
    }

    pub(super) fn remove_unresolved_client_order_route(&mut self, client_order_id: &str) {
        if self
            .pending_route_by_client_order_id
            .remove(client_order_id)
            .is_some()
        {
            self.pending_cancel_client_order_ids.remove(client_order_id);
        }
    }

    pub(super) fn record_mapping(
        &mut self,
        route: TbankBrokerOrderRoute,
        client_order_id: &str,
        venue_order_id: &str,
    ) -> bool {
        if venue_order_id.is_empty() {
            return false;
        }
        self.record_venue_order_id(route, venue_order_id);
        let mut should_cancel = false;
        if !client_order_id.is_empty() {
            should_cancel = self.pending_cancel_client_order_ids.remove(client_order_id);
            self.pending_route_by_client_order_id
                .remove(client_order_id);
            self.client_order_id_by_venue_order_id
                .insert(venue_order_id.to_string(), client_order_id.to_string());
            self.by_client_order_id.insert(
                client_order_id.to_string(),
                TbankBrokerOrderIdentity {
                    route,
                    broker_order_id: venue_order_id.to_string(),
                },
            );
        }
        should_cancel
    }

    pub(super) fn record_activated_stop_child_mapping(
        &mut self,
        client_order_id: &str,
        stop_order_id: &str,
        child_order_id: &str,
    ) -> bool {
        if child_order_id.is_empty() || child_order_id == stop_order_id {
            return false;
        }
        let should_cancel = self.record_mapping(
            TbankBrokerOrderRoute::RegularOrder,
            client_order_id,
            child_order_id,
        );
        self.record_activated_stop_child_alias(stop_order_id, child_order_id);
        should_cancel
    }

    pub(super) fn record_activated_stop_child_alias(
        &mut self,
        stop_order_id: &str,
        child_order_id: &str,
    ) {
        if child_order_id.is_empty() || child_order_id == stop_order_id {
            return;
        }
        self.record_venue_order_id(TbankBrokerOrderRoute::RegularOrder, child_order_id);
        if !stop_order_id.is_empty() {
            self.canonical_venue_order_id_by_alias
                .insert(child_order_id.to_string(), stop_order_id.to_string());
        }
    }

    pub(super) fn record_regular_order_alias(
        &mut self,
        client_order_id: &str,
        canonical_order_id: &str,
        current_order_id: &str,
    ) -> bool {
        if current_order_id.is_empty() || current_order_id == canonical_order_id {
            return false;
        }
        let should_cancel = self.record_mapping(
            TbankBrokerOrderRoute::RegularOrder,
            client_order_id,
            current_order_id,
        );
        if !canonical_order_id.is_empty() {
            self.canonical_venue_order_id_by_alias
                .insert(current_order_id.to_string(), canonical_order_id.to_string());
        }
        should_cancel
    }

    pub(super) fn get_or_allocate_request_mapping(
        &mut self,
        client_order_id: &str,
        supplied_broker_request_id: Option<&str>,
    ) -> std::result::Result<String, String> {
        if client_order_id.is_empty() {
            return Err("client_order_id must not be empty".to_string());
        }
        if let Some(existing) = self
            .broker_request_id_by_client_order_id
            .get(client_order_id)
        {
            return Ok(existing.clone());
        }
        let broker_request_id = supplied_broker_request_id
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| tbank_broker_request_id_for_client_order_id(client_order_id));
        let parsed = Uuid::parse_str(broker_request_id.as_str())
            .map_err(|error| format!("broker request id must be a UUID: {error}"))?;
        if parsed.to_string() != broker_request_id {
            return Err(format!(
                "broker request id must use canonical lowercase UUID form: {broker_request_id}"
            ));
        }
        if let Some(existing_client_order_id) = self
            .client_order_id_by_request_id
            .get(broker_request_id.as_str())
            && existing_client_order_id != client_order_id
        {
            return Err(format!(
                "broker request id {broker_request_id} is already mapped to client order {existing_client_order_id}"
            ));
        }
        self.broker_request_id_by_client_order_id
            .insert(client_order_id.to_string(), broker_request_id.clone());
        self.client_order_id_by_request_id
            .insert(broker_request_id.clone(), client_order_id.to_string());
        Ok(broker_request_id)
    }

    pub(super) fn client_order_id_for_request_id(&self, broker_request_id: &str) -> Option<String> {
        self.client_order_id_by_request_id
            .get(broker_request_id)
            .cloned()
    }

    pub(super) fn is_known_regular_order_request_id(&self, broker_request_id: &str) -> bool {
        let client_order_id = self.client_order_id_for_request_id(broker_request_id);
        client_order_id
            .as_deref()
            .and_then(|client_order_id| self.route_for_client_order_id(client_order_id))
            .is_some_and(|route| route == TbankBrokerOrderRoute::RegularOrder)
    }

    pub(super) fn record_venue_order_id(
        &mut self,
        route: TbankBrokerOrderRoute,
        venue_order_id: &str,
    ) {
        if !venue_order_id.is_empty() {
            self.by_venue_order_id
                .insert(venue_order_id.to_string(), route);
            if route == TbankBrokerOrderRoute::StopOrder {
                self.stop_broker_order_ids
                    .insert(venue_order_id.to_string());
            }
        }
    }

    pub(super) fn identity_for(
        &self,
        client_order_id: Option<&str>,
        venue_order_id: Option<&str>,
    ) -> Option<TbankBrokerOrderIdentity> {
        if let Some(venue_order_id) = venue_order_id
            && let Some(route) = self.by_venue_order_id.get(venue_order_id).copied()
        {
            return Some(TbankBrokerOrderIdentity {
                route,
                broker_order_id: venue_order_id.to_string(),
            });
        }
        client_order_id
            .and_then(|client_order_id| self.by_client_order_id.get(client_order_id).cloned())
    }

    pub(super) fn route_for_client_order_id(
        &self,
        client_order_id: &str,
    ) -> Option<TbankBrokerOrderRoute> {
        self.by_client_order_id
            .get(client_order_id)
            .map(|identity| identity.route)
            .or_else(|| {
                self.pending_route_by_client_order_id
                    .get(client_order_id)
                    .copied()
            })
    }

    pub(super) fn client_order_id_for_venue_order_id(
        &self,
        venue_order_id: &str,
    ) -> Option<String> {
        self.client_order_id_by_venue_order_id
            .get(venue_order_id)
            .cloned()
    }

    pub(super) fn canonical_venue_order_identity(
        &self,
        venue_order_id: &str,
    ) -> Option<(String, Option<String>)> {
        if !self.by_venue_order_id.contains_key(venue_order_id)
            && !self
                .canonical_venue_order_id_by_alias
                .contains_key(venue_order_id)
        {
            return None;
        }
        let canonical_venue_order_id = self
            .canonical_venue_order_id_by_alias
            .get(venue_order_id)
            .cloned()
            .unwrap_or_else(|| venue_order_id.to_string());
        let client_order_id = self
            .client_order_id_by_venue_order_id
            .get(venue_order_id)
            .or_else(|| {
                self.client_order_id_by_venue_order_id
                    .get(canonical_venue_order_id.as_str())
            })
            .cloned();
        Some((canonical_venue_order_id, client_order_id))
    }

    pub(super) fn canonical_venue_order_id_or_self(&self, venue_order_id: &str) -> String {
        self.canonical_venue_order_id_by_alias
            .get(venue_order_id)
            .cloned()
            .unwrap_or_else(|| venue_order_id.to_string())
    }

    pub(super) fn has_activated_stop_child_mapping(&self, stop_order_id: &str) -> bool {
        self.canonical_venue_order_id_by_alias
            .values()
            .any(|canonical_venue_order_id| canonical_venue_order_id == stop_order_id)
    }

    pub(super) fn aliases_for_canonical_venue_order_id(
        &self,
        canonical_order_id: &str,
    ) -> Vec<String> {
        self.canonical_venue_order_id_by_alias
            .iter()
            .filter(|(_, canonical)| canonical.as_str() == canonical_order_id)
            .map(|(alias, _)| alias.clone())
            .collect()
    }

    pub(super) fn record_managed_context(
        &mut self,
        client_order_id: &str,
        context: TbankManagedOrderContext,
    ) {
        if !client_order_id.is_empty() {
            self.managed_context_by_client_order_id
                .insert(client_order_id.to_string(), context);
        }
    }

    pub(super) fn managed_context_for_client_order_id(
        &self,
        client_order_id: &str,
    ) -> Option<TbankManagedOrderContext> {
        self.managed_context_by_client_order_id
            .get(client_order_id)
            .cloned()
    }

    pub(super) fn known_regular_broker_order_ids(&self) -> Vec<String> {
        let mut order_ids = self
            .by_client_order_id
            .values()
            .filter(|identity| identity.route == TbankBrokerOrderRoute::RegularOrder)
            .map(|identity| identity.broker_order_id.clone())
            .collect::<HashSet<_>>();
        order_ids.extend(
            self.by_venue_order_id
                .iter()
                .filter(|(_, route)| **route == TbankBrokerOrderRoute::RegularOrder)
                .map(|(order_id, _)| order_id.clone()),
        );
        order_ids.into_iter().collect()
    }

    pub(super) fn unresolved_regular_request_mappings(&self) -> Vec<(String, String)> {
        self.broker_request_id_by_client_order_id
            .iter()
            .filter(|(client_order_id, _)| {
                self.pending_route_by_client_order_id.get(*client_order_id)
                    == Some(&TbankBrokerOrderRoute::RegularOrder)
            })
            .map(|(client_order_id, broker_request_id)| {
                (client_order_id.clone(), broker_request_id.clone())
            })
            .collect()
    }

    pub(super) fn known_stop_broker_order_ids(&self) -> Vec<String> {
        self.stop_broker_order_ids.iter().cloned().collect()
    }

    pub(super) fn is_known_stop_broker_order_id(&self, venue_order_id: &str) -> bool {
        self.stop_broker_order_ids.contains(venue_order_id)
    }

    pub(super) fn record_pending_cancel(&mut self, client_order_id: &str) {
        if !client_order_id.is_empty() {
            self.pending_cancel_client_order_ids
                .insert(client_order_id.to_string());
        }
    }
}
