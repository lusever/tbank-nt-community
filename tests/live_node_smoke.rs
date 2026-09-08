use nautilus_common::enums::Environment;
use nautilus_live::{config::RoutingConfig, node::LiveNode};
use nautilus_model::identifiers::TraderId;
use tbank_nt_community::{
    TbankDataClientConfig, TbankDataClientFactory, TbankEnvironment, TbankExecutionClientConfig,
    TbankExecutionClientFactory,
};

#[test]
fn live_node_builds_with_tbank_factories_and_configs() {
    let trader_id = TraderId::from("TRADER-001");
    let endpoint = "http://127.0.0.1:1".to_string();
    let data_config = TbankDataClientConfig {
        environment: TbankEnvironment::Sandbox,
        token: Some("test-token".to_string()),
        endpoint: Some(endpoint.clone()),
        ..TbankDataClientConfig::default()
    };
    let execution_config = TbankExecutionClientConfig {
        environment: TbankEnvironment::Sandbox,
        token: Some("test-token".to_string()),
        account_id: Some("test-account".to_string()),
        endpoint: Some(endpoint),
        ..TbankExecutionClientConfig::default()
    };

    let routing = RoutingConfig::builder()
        .default(true)
        .venues(vec!["MOEX".to_string(), "SPBE".to_string()])
        .build();
    let node = LiveNode::builder(trader_id, Environment::Sandbox)
        .unwrap()
        .with_name("tbank-adapter-smoke")
        .add_data_client_with_routing(
            Some("tbank".to_string()),
            Box::new(TbankDataClientFactory::new()),
            Box::new(data_config),
            routing.clone(),
        )
        .unwrap()
        .add_exec_client_with_routing(
            Some("tbank".to_string()),
            Box::new(TbankExecutionClientFactory::new()),
            Box::new(execution_config),
            routing,
        )
        .unwrap()
        .build();

    assert!(node.is_ok());
}
