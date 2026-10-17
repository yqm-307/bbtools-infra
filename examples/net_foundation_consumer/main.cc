#include <bbt/infra/net/NativeSocket.hpp>

#include <poll.h>

#include <cstring>
#include <iostream>

int main() {
    using namespace bbt::infra::net;
    std::error_code error;
    const auto address = Address::Ip("127.0.0.1", 0, error);
    if (error) { std::cerr << error.message(); return 1; }
    auto receiver = NativeSocket::Open(AddressFamily::IPv4, SocketType::Datagram, error);
    if (error) { std::cerr << error.message(); return 1; }
    error = receiver.Bind(address);
    if (error) { std::cerr << error.message(); return 1; }
    const auto bound = receiver.LocalAddress(error);
    if (error) { std::cerr << error.message(); return 1; }
    auto sender = NativeSocket::Open(AddressFamily::IPv4, SocketType::Datagram, error);
    if (error) { std::cerr << error.message(); return 1; }
    const char payload[] = "public-net-consumer";
    const auto sent = sender.SendTo(payload, sizeof(payload), bound);
    if (sent.error || sent.bytes != sizeof(payload)) { std::cerr << "send failed"; return 1; }
    pollfd ready{receiver.Fd(), POLLIN, 0};
    if (::poll(&ready, 1, 2000) != 1) { std::cerr << "readiness failed"; return 1; }
    char bytes[64]{};
    Address peer;
    const auto received = receiver.ReceiveFrom(bytes, sizeof(bytes), peer);
    if (received.error || received.truncated || received.bytes != sizeof(payload)
        || std::memcmp(payload, bytes, sizeof(payload)) != 0) {
        std::cerr << "roundtrip failed"; return 1;
    }
    std::cout << "public consumer UDP roundtrip: " << bound.ToString() << '\n';
    if (receiver.Close() || sender.Close()) { std::cerr << "close failed"; return 1; }
    return 0;
}
