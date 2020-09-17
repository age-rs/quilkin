/*
 * Copyright 2020 Google LLC All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

extern crate quilkin;

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use serde_yaml::{Mapping, Value};
    use slog::info;

    use quilkin::config::{Config, ConnectionConfig, EndPoint, Filter, Local};
    use quilkin::extensions::filters::DebugFilterFactory;
    use quilkin::extensions::{default_registry, FilterFactory};
    use quilkin::test_utils::{TestFilterFactory, TestHelper};

    #[tokio::test]
    async fn test_filter() {
        let mut t = TestHelper::default();

        // create an echo server as an endpoint.
        let echo = t.run_echo_server().await;

        // create server configuration
        let server_port = 12346;
        let server_config = Config {
            local: Local { port: server_port },
            filters: vec![Filter {
                name: "TestFilter".to_string(),
                config: None,
            }],
            connections: ConnectionConfig::Server {
                endpoints: vec![EndPoint {
                    name: "server".to_string(),
                    address: echo,
                    connection_ids: vec![],
                }],
            },
        };
        assert_eq!(Ok(()), server_config.validate());

        // Run server proxy.
        let mut registry = default_registry(&t.log);
        registry.insert(TestFilterFactory {});
        t.run_server_with_filter_registry(server_config, registry);

        // create a local client
        let client_port = 12347;
        let client_config = Config {
            local: Local { port: client_port },
            filters: vec![Filter {
                name: "TestFilter".to_string(),
                config: None,
            }],
            connections: ConnectionConfig::Client {
                addresses: vec![SocketAddr::new(
                    IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
                    server_port,
                )],
                connection_id: "".into(),
                lb_policy: None,
            },
        };
        assert_eq!(Ok(()), client_config.validate());

        // Run client proxy.
        let mut registry = default_registry(&t.log);
        registry.insert(TestFilterFactory {});
        t.run_server_with_filter_registry(client_config, registry);

        // let's send the packet
        let (mut recv_chan, mut send) = t.open_socket_and_recv_multiple_packets().await;

        // game_client
        let local_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), client_port);
        info!(t.log, "Sending hello"; "addr" => local_addr);
        send.send_to("hello".as_bytes(), &local_addr).await.unwrap();

        let result = recv_chan.recv().await.unwrap();
        // since we don't know the ephemeral ip addresses in use, we'll search for
        // substrings for the results we expect that the TestFilter will inject in
        // the round-tripped packets.
        assert_eq!(
            2,
            result.matches("odr").count(),
            "Should be 2 on_downstream_receive calls in {}",
            result
        );
        assert_eq!(
            2,
            result.matches("our").count(),
            "Should be 2 on_upstream_receive calls in {}",
            result
        );
    }

    #[tokio::test]
    async fn debug_filter() {
        let mut t = TestHelper::default();

        // handy for grabbing the configuration name
        let factory = DebugFilterFactory::new(&t.log);

        // create an echo server as an endpoint.
        let echo = t.run_echo_server().await;

        // filter config
        let mut map = Mapping::new();
        map.insert(Value::from("id"), Value::from("server"));
        // create server configuration
        let server_port = 12247;
        let server_config = Config {
            local: Local { port: server_port },
            filters: vec![Filter {
                name: factory.name(),
                config: Some(serde_yaml::Value::Mapping(map)),
            }],
            connections: ConnectionConfig::Server {
                endpoints: vec![EndPoint {
                    name: "server".to_string(),
                    address: echo,
                    connection_ids: vec![],
                }],
            },
        };
        t.run_server(server_config);

        let mut map = Mapping::new();
        map.insert(Value::from("id"), Value::from("client"));
        // create a local client
        let client_port = 12248;
        let client_config = Config {
            local: Local { port: client_port },
            filters: vec![Filter {
                name: factory.name(),
                config: Some(serde_yaml::Value::Mapping(map)),
            }],
            connections: ConnectionConfig::Client {
                addresses: vec![SocketAddr::new(
                    IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
                    server_port,
                )],
                connection_id: "".into(),
                lb_policy: None,
            },
        };
        t.run_server(client_config);

        // let's send the packet
        let (mut recv_chan, mut send) = t.open_socket_and_recv_multiple_packets().await;

        // game client
        let local_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), client_port);
        info!(t.log, "Sending hello"; "addr" => local_addr);
        send.send_to("hello".as_bytes(), &local_addr).await.unwrap();

        // since the debug filter doesn't change the data, it should be exactly the same
        assert_eq!("hello", recv_chan.recv().await.unwrap());
    }
}