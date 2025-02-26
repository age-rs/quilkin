/*
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use tokio::time::{Duration, timeout};

use quilkin::{
    config::Filter,
    filters::{Capture, Match, StaticFilter},
    net::endpoint::Endpoint,
    test::{AddressType, TestHelper},
};

#[tokio::test]
#[cfg_attr(target_os = "macos", ignore)]
async fn r#match() {
    let mut t = TestHelper::default();
    let echo = t.run_echo_server(AddressType::Random).await;

    let capture_yaml = "
suffix:
    size: 3
    remove: true
";

    let matches_yaml = "
on_read:
    metadataKey: quilkin.dev/capture
    fallthrough:
        name: quilkin.filters.concatenate.v1alpha1.Concatenate
        config:
            on_read: APPEND
            bytes: ZGVm
    branches:
        - value: abc
          name: quilkin.filters.concatenate.v1alpha1.Concatenate
          config:
            on_read: APPEND
            bytes: eHl6 # xyz
        - value: xyz
          name: quilkin.filters.concatenate.v1alpha1.Concatenate
          config:
            on_read: APPEND
            bytes: YWJj # abc
";

    let server_config = std::sync::Arc::new(quilkin::Config::default_non_agent());
    server_config
        .dyn_cfg
        .clusters()
        .unwrap()
        .modify(|clusters| clusters.insert_default([Endpoint::new(echo.clone())].into()));
    server_config.dyn_cfg.filters().unwrap().store(
        quilkin::filters::FilterChain::try_create([
            Filter {
                name: Capture::NAME.into(),
                label: None,
                config: serde_yaml::from_str(capture_yaml).unwrap(),
            },
            Filter {
                name: Match::NAME.into(),
                label: None,
                config: serde_yaml::from_str(matches_yaml).unwrap(),
            },
        ])
        .map(std::sync::Arc::new)
        .unwrap(),
    );

    let server_port = t.run_server(server_config, None, None).await;

    let (mut recv_chan, socket) = t.open_socket_and_recv_multiple_packets().await;

    let local_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), server_port);

    // abc packet
    let msg = b"helloabc";
    socket.send_to(msg, &local_addr).await.unwrap();

    assert_eq!(
        "helloxyz",
        timeout(Duration::from_millis(500), recv_chan.recv())
            .await
            .expect("should have received a packet")
            .unwrap()
    );

    // send an xyz packet
    let msg = b"helloxyz";
    socket.send_to(msg, &local_addr).await.unwrap();

    assert_eq!(
        "helloabc",
        timeout(Duration::from_millis(500), recv_chan.recv())
            .await
            .expect("should have received a packet")
            .unwrap()
    );

    // fallthrough packet
    let msg = b"hellodef";
    socket.send_to(msg, &local_addr).await.unwrap();

    assert_eq!(
        "hellodef",
        timeout(Duration::from_millis(500), recv_chan.recv())
            .await
            .expect("should have received a packet")
            .unwrap()
    );

    // second fallthrough packet
    let msg = b"hellofgh";
    socket.send_to(msg, &local_addr).await.unwrap();

    assert_eq!(
        "hellodef",
        timeout(Duration::from_millis(500), recv_chan.recv())
            .await
            .expect("should have received a packet")
            .unwrap()
    );
}
