/*
 * Copyright 2020 Google LLC
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

use tokio::time::Duration;
use tokio::time::timeout;

use quilkin::{
    net::endpoint::Endpoint,
    test::{AddressType, TestHelper},
};

#[tokio::test]
#[cfg_attr(target_os = "macos", ignore)]
async fn echo() {
    let mut t = TestHelper::default();

    // create two echo servers as endpoints
    let server1 = t.run_echo_server(AddressType::Random).await;
    let server2 = t.run_echo_server(AddressType::Random).await;

    // create server configuration
    let server_config = std::sync::Arc::new(quilkin::Config::default_non_agent());
    server_config
        .dyn_cfg
        .clusters()
        .unwrap()
        .modify(|clusters| {
            clusters.insert_default(
                [
                    Endpoint::new(server1.clone()),
                    Endpoint::new(server2.clone()),
                ]
                .into(),
            );
        });

    let local_port = t.run_server(server_config, None, None).await;
    let local_addr = std::net::SocketAddr::from((std::net::Ipv6Addr::LOCALHOST, local_port));

    // let's send the packet
    let (mut recv_chan, socket) = t.open_socket_and_recv_multiple_packets().await;

    socket.send_to(b"hello", &local_addr).await.unwrap();
    let value = timeout(Duration::from_millis(500), recv_chan.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!("hello", value);
    let value = timeout(Duration::from_millis(500), recv_chan.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!("hello", value);

    // should only be two returned items
    assert!(
        timeout(Duration::from_millis(500), recv_chan.recv())
            .await
            .is_err()
    );
}
