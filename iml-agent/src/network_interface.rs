// Copyright (c) 2020 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

use crate::{agent_error::ImlAgentError, network_interface_stats};
use combine::{
    attempt, choice,
    error::ParseError,
    many1, optional,
    parser::{
        char::{char, digit, letter, spaces, string},
        repeat::take_until,
    },
    sep_by1,
    stream::Stream,
    token, Parser,
};
use iml_wire_types::{LinuxType, LndType, NetworkInterface};
use ipnetwork::{Ipv4Network, Ipv6Network};
use std::{
    collections::HashMap,
    convert::TryFrom,
    net::{Ipv4Addr, Ipv6Addr},
    num::ParseIntError,
};

#[derive(Debug, PartialEq, Eq)]
pub enum InterfaceProperties {
    InterfaceFlagsAndAttributes((String, Vec<String>, HashMap<String, String>)),
    InterfaceTypeAndMacAddress((Option<String>, Option<String>)),
    Inet4AddressAndPrefix((String, Result<u8, ParseIntError>)),
    Inet6AddressAndPrefix((String, Result<u8, ParseIntError>)),
}

trait SetNetworkProperty {
    type Prop;
    type Error;

    fn set_prop(self, prop: Self::Prop) -> Result<NetworkInterface, Self::Error>;
}

impl SetNetworkProperty for NetworkInterface {
    type Prop = InterfaceProperties;
    type Error = ImlAgentError;

    fn set_prop(mut self, prop: InterfaceProperties) -> Result<NetworkInterface, ImlAgentError> {
        match prop {
            InterfaceProperties::InterfaceFlagsAndAttributes((interface, flags, _attributes)) => {
                self.interface = interface;

                if flags.iter().any(|x| x == &"UP".to_string()) {
                    self.is_up = true;
                }

                if flags.iter().any(|x| x == &"SLAVE".to_string()) {
                    self.is_slave = true;
                }
            }
            InterfaceProperties::InterfaceTypeAndMacAddress((interface_type, address)) => {
                let interface_type: Option<LinuxType> = LinuxType::try_from(interface_type).ok();
                let interface_type: Option<LndType> = LndType::try_from(interface_type).ok();
                self.interface_type = interface_type;
                self.mac_address = address;
            }
            InterfaceProperties::Inet4AddressAndPrefix((address, prefix)) => {
                let address: Ipv4Addr = address.parse()?;
                self.inet4_address.push(Ipv4Network::new(address, prefix?)?);
            }
            InterfaceProperties::Inet6AddressAndPrefix((address, prefix)) => {
                let address: Ipv6Addr = address.parse()?;
                self.inet6_address.push(Ipv6Network::new(address, prefix?)?);
            }
        }

        Ok(self)
    }
}

fn interface_start<I>() -> impl Parser<I, Output = Result<u32, ParseIntError>>
where
    I: Stream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
{
    spaces()
        .and(many1::<Vec<_>, _, _>(digit()))
        .map(|x| x.1.into_iter().collect::<String>().parse::<u32>())
        .skip(char(':'))
}

fn interface<I>() -> impl Parser<I, Output = String>
where
    I: Stream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
{
    many1(letter().or(digit()).or(token('@')))
}

fn parse_interface<I>() -> impl Parser<I, Output = String>
where
    I: Stream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
{
    spaces()
        .and(many1::<Vec<_>, _, _>(digit()))
        .skip(char(':'))
        .skip(spaces())
        .and(interface())
        .skip(token(':'))
        .map(|(_, x)| x)
}

fn parse_type<I>() -> impl Parser<I, Output = Option<String>>
where
    I: Stream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
{
    spaces()
        .skip(string("link"))
        .skip(char('/'))
        .and(optional(many1(letter())))
        .map(|x| x.1)
}

fn parse_mac_address<I>() -> impl Parser<I, Output = Option<String>>
where
    I: Stream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
{
    spaces().and(optional(take_until(char(' ')))).map(|x| x.1)
}

fn parse_inet4_address<I>() -> impl Parser<I, Output = String>
where
    I: Stream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
{
    spaces()
        .skip(string("inet "))
        .skip(spaces())
        .and(take_until(char('/')))
        .map(|x| x.1)
}

fn parse_inet4_prefix<I>() -> impl Parser<I, Output = Result<u8, ParseIntError>>
where
    I: Stream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
{
    char('/')
        .and(many1(digit()).map(|x: String| x.parse::<u8>()))
        .map(|x| x.1)
}

fn parse_inet6_address<I>() -> impl Parser<I, Output = String>
where
    I: Stream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
{
    spaces()
        .skip(string("inet6"))
        .skip(spaces())
        .and(take_until(char('/')))
        .map(|x| x.1)
}

fn parse_inet6_prefix<I>() -> impl Parser<I, Output = Result<u8, ParseIntError>>
where
    I: Stream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
{
    char('/')
        .and(many1(digit()).map(|x: String| x.parse::<u8>()))
        .map(|x| x.1)
}

fn word<I>() -> impl Parser<I, Output = String>
where
    I: Stream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
{
    many1(letter().or(digit()).or(token('_')).or(token('-')))
}

fn parse_flags<I>() -> impl Parser<I, Output = Vec<String>>
where
    I: Stream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
{
    spaces()
        .skip(token('<'))
        .and(sep_by1(word(), token(',')))
        .skip(token('>'))
        .map(|x| x.1)
}

fn parse_attributes<I>() -> impl Parser<I, Output = HashMap<String, String>>
where
    I: Stream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
{
    spaces()
        .and(sep_by1(word(), char(' ')))
        .map(|(_, y): (_, Vec<String>)| {
            y.chunks(2)
                .map(|x| (x[0].to_string(), x[1].to_string()))
                .collect::<HashMap<String, String>>()
        })
}

fn parse_network_line<I>() -> impl Parser<I, Output = InterfaceProperties>
where
    I: Stream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
{
    choice((
        attempt(
            parse_interface()
                .and(parse_flags())
                .and(parse_attributes())
                .map(|((interface, flags), attributes)| {
                    InterfaceProperties::InterfaceFlagsAndAttributes((interface, flags, attributes))
                }),
        ),
        attempt(
            parse_type()
                .and(parse_mac_address())
                .map(InterfaceProperties::InterfaceTypeAndMacAddress),
        ),
        attempt(
            parse_inet4_address()
                .and(parse_inet4_prefix())
                .map(InterfaceProperties::Inet4AddressAndPrefix),
        ),
        attempt(
            parse_inet6_address()
                .and(parse_inet6_prefix())
                .map(InterfaceProperties::Inet6AddressAndPrefix),
        ),
    ))
}

pub fn parse(
    output: &str,
    mut stats_map: network_interface_stats::InterfaceStatsMap,
) -> Result<Vec<NetworkInterface>, ImlAgentError> {
    let xs = output.split('\n').fold(vec![], |mut acc, x| {
        if interface_start().parse(x).map(|x| x.0).is_ok() {
            let iface = vec![x];
            acc.push(iface);
        } else if let Some(cur_iface) = acc.last_mut() {
            cur_iface.push(x);
        }

        acc
    });

    xs.into_iter()
        .map(|x| {
            x.into_iter()
                .filter_map(|x| parse_network_line().parse(x).ok())
                .try_fold(NetworkInterface::default(), |acc, (x, _)| acc.set_prop(x))
        })
        .filter(|x| {
            if let Ok(x) = x {
                return x.interface.trim() != "lo" && !x.is_slave;
            }

            true
        })
        .map(|x| {
            if let Ok(mut x) = x {
                let interface: Vec<&str> = x.interface.split('@').collect();

                if let Some(stats) = stats_map.get_mut(interface[0]) {
                    x.stats = Some(stats.clone());
                }

                return Ok(x);
            }

            x
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_interface() {
        let result  = parse_network_line()
            .parse("1: lo: <LOOPBACK,UP,LOWER_UP> mtu 65536 qdisc noqueue state UNKNOWN group default qlen 1000")
            .map(|x| x.0);

        let interface =
            if let Ok(InterfaceProperties::InterfaceFlagsAndAttributes((interface, _, _))) = result
            {
                interface
            } else {
                "".to_string()
            };

        assert_eq!(interface, "lo".to_string());
    }

    #[test]
    fn test_parse_type() {
        let result = parse_network_line()
            .parse("     link/loopback 00:00:00:00:00:00 brd 00:00:00:00:00:00")
            .map(|x| x.0);

        let iface_type =
            if let Ok(InterfaceProperties::InterfaceTypeAndMacAddress((iface_type, _))) = result {
                iface_type
            } else {
                None
            };

        assert_eq!(iface_type, Some("loopback".to_string()));
    }

    #[test]
    fn test_parse_mac_address() {
        let result = parse_network_line()
            .parse("link/ether ae:b5:89:49:2d:77 brd ff:ff:ff:ff:ff:ff link-netnsid 4")
            .map(|x| x.0);

        let mac_address =
            if let Ok(InterfaceProperties::InterfaceTypeAndMacAddress((_, mac_address))) = result {
                mac_address
            } else {
                None
            };

        assert_eq!(mac_address, Some("ae:b5:89:49:2d:77".to_string()));
    }

    #[test]
    fn test_parse_inet4_address() {
        let result = parse_network_line()
            .parse("    inet 127.0.0.1/8 scope host lo")
            .map(|x| x.0);

        let address = if let Ok(InterfaceProperties::Inet4AddressAndPrefix((address, _))) = result {
            address
        } else {
            "".to_string()
        };

        assert_eq!(address, "127.0.0.1".to_string());
    }

    #[test]
    fn test_parse_inet4_prefix() {
        let result = parse_network_line()
            .parse("    inet 127.0.0.1/8 scope host lo")
            .map(|x| x.0);

        let prefix = if let Ok(InterfaceProperties::Inet4AddressAndPrefix((_, prefix))) = result {
            prefix
        } else {
            " ".parse::<u8>()
        };

        assert_eq!(prefix, Ok(8));
    }

    #[test]
    fn test_parse_inet6_address() {
        let result = parse_network_line()
            .parse("inet6 fe80::acb5:89ff:fe49:2d77/64 scope link")
            .map(|x| x.0);

        let address = if let Ok(InterfaceProperties::Inet6AddressAndPrefix((address, _))) = result {
            address
        } else {
            "".to_string()
        };

        assert_eq!(address, "fe80::acb5:89ff:fe49:2d77".to_string());
    }

    #[test]
    fn test_parse_inet6_prefix() {
        let result = parse_network_line()
            .parse("inet6 fe80::acb5:89ff:fe49:2d77/64 scope link")
            .map(|x| x.0);

        let prefix = if let Ok(InterfaceProperties::Inet6AddressAndPrefix((_, prefix))) = result {
            prefix
        } else {
            " ".parse::<u8>()
        };

        assert_eq!(prefix, Ok(64));
    }

    #[test]
    fn test_parse_flags() {
        let result = parse_network_line()
            .parse("35586: vethd5ae58b@if35585: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500 qdisc noqueue master docker_gwbridge state UP group default")
            .map(|x| x.0);

        let flags =
            if let Ok(InterfaceProperties::InterfaceFlagsAndAttributes((_, flags, _))) = result {
                flags
            } else {
                vec![]
            };

        assert_eq!(
            flags,
            vec![
                "BROADCAST".to_string(),
                "MULTICAST".to_string(),
                "UP".to_string(),
                "LOWER_UP".to_string(),
            ]
        );
    }

    #[test]
    fn test_parse_attributes() {
        let result = parse_network_line()
            .parse("35586: vethd5ae58b@if35585: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500 qdisc noqueue master docker_gwbridge state UP group default")
            .map(|x| x.0);

        let attributes = if let Ok(InterfaceProperties::InterfaceFlagsAndAttributes((
            _,
            _,
            attributes,
        ))) = result
        {
            attributes
        } else {
            HashMap::new()
        };

        assert_eq!(
            attributes,
            vec![
                ("mtu".to_string(), "1500".to_string()),
                ("qdisc".to_string(), "noqueue".to_string()),
                ("master".to_string(), "docker_gwbridge".to_string()),
                ("state".to_string(), "UP".to_string()),
                ("group".to_string(), "default".to_string())
            ]
            .iter()
            .cloned()
            .collect::<HashMap<String, String>>()
        );
    }

    #[test]
    fn test_parse_error() {
        let result = parse_network_line()
            .parse("inet6 fe80::acb5:89ff:fe49:2d77/888888884 scope link")
            .map(|x| x.0);

        println!("Result: {:?}", result);
        if let Ok(InterfaceProperties::Inet6AddressAndPrefix((_, x))) = result {
            assert_eq!(x, "8888888".parse::<u8>());
        } else {
            assert!(false);
        };
    }

    #[test]
    fn test_parse_unknown_interface_type() {
        let result = parse("2: usermode: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500 qdisc pfifo_fast state UP group default qlen 1000
link/unknown 52:54:00:12:34:56 brd ff:ff:ff:ff:ff:ff
inet 10.0.2.15/24 brd 10.0.2.255 scope global dynamic usermode
    valid_lft 65748sec preferred_lft 65748sec
inet6 fec0::5054:ff:fe12:3456/64 scope site mngtmpaddr dynamic
    valid_lft 86267sec preferred_lft 14267sec
inet6 fe80::5054:ff:fe12:3456/64 scope link
    valid_lft forever preferred_lft forever", HashMap::new());

        if let Ok(x) = result {
            assert!(x[0].interface_type.is_none())
        } else {
            assert!(false);
        };
    }

    #[test]
    fn test_parsing_multiple_interfaces() {
        let network_interfaces = include_bytes!("./fixtures/network_interfaces.txt");
        let network_interfaces = std::str::from_utf8(network_interfaces).unwrap();

        let stats = include_bytes!("./fixtures/network_stats.txt");
        let stats = std::str::from_utf8(stats).unwrap();
        let stats_map = network_interface_stats::parse(stats).unwrap();

        let network_interfaces = parse(network_interfaces, stats_map);

        insta::assert_json_snapshot!(network_interfaces);
    }
}
