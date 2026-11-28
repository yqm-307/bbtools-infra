#include <bbt/infra/consul/detail/ClientState.hpp>

#include <bbt/infra/consul/detail/ClientTestAccess.hpp>

#include <algorithm>
#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/infra/CloseOptions.hpp>

namespace bbt::infra::consul {

Client::State::State(std::shared_ptr<bbt::infra::http::Client> client, Options opts)
    : http(std::move(client))
    , options(std::move(opts))
{
}

std::chrono::milliseconds Client::State::ReadBudgetForQuery(bool blocking) const
{
    if (!blocking || options.query_wait <= std::chrono::milliseconds{0}) {
        return options.request_timeout;
    }
    const auto jitter = std::chrono::milliseconds{(options.query_wait.count() + 15) / 16};
    const auto budget = options.query_wait + jitter + std::chrono::milliseconds{250};
    return std::min(options.request_timeout, budget);
}

bool Client::State::MarkTerminalLocked(std::vector<bbt::coroutine::sync::CoWaiter::SPtr>* woken)
{
    if (terminal || inflight != 0 || !closed.load() || !http_closed) {
        return false;
    }
    terminal = true;
    woken->insert(woken->end(), co_waiters.begin(), co_waiters.end());
    co_waiters.clear();
    return true;
}

bool Client::State::MarkSettledLocked(std::vector<bbt::coroutine::sync::CoWaiter::SPtr>* woken)
{
    if (!closed.load() || inflight != 0) {
        return false;
    }
    woken->insert(woken->end(), co_waiters.begin(), co_waiters.end());
    co_waiters.clear();
    return true;
}

bool Client::State::BeginHttpCloseLocked()
{
    if (!closed.load() || (!immediate && inflight != 0) || http_close_started) {
        return false;
    }
    http_close_started = true;
    return true;
}

bbt_co_async(result<http::Response>) Client::State::Send(
    std::string_view method,
    const std::string& target,
    std::string body,
    std::chrono::milliseconds read_budget) const
{
    http::RequestOptions request;
    request.headers.emplace_back("Accept", "application/json");
    if (!options.token.empty()) {
        request.headers.emplace_back("X-Consul-Token", options.token);
    }
    request.SetTimeout(options.request_timeout).SetReadTimeout(read_budget);

    auto prepared = http->BuildRequest(method, base + target, request);
    if (!prepared) {
        bbt_co_return result<http::Response>::err(std::move(prepared.error()));
    }
    if (!body.empty()) {
        prepared.value().body = std::move(body);
        prepared.value().headers.emplace_back("Content-Type", "application/json");
    }
    CallOptions call;
    call.deadline = std::chrono::steady_clock::now() + options.request_timeout;
    auto response = bbt_co_await http->Send(prepared.value(), call);
    bbt_co_return std::move(response);
}

std::shared_ptr<HealthDeliveryPause> Client::State::TakeHealthDeliveryPause()
{
    std::lock_guard<std::mutex> lock(mutex);
    return std::exchange(health_delivery_pause, {}).lock();
}

std::shared_ptr<HealthDeliveryPause> ClientTestAccess::PauseHealthDelivery(const Client& client)
{
    auto pause = std::make_shared<HealthDeliveryPause>();
    const auto& state = client.m_state;
    std::lock_guard<std::mutex> lock(state->mutex);
    state->health_delivery_pause = pause;
    return pause;
}

void Client::State::CompleteHttpClose() noexcept
{
    (void)http->Close(CloseOptions::Immediate(MakeError(ErrorCode::Closed, "consul: owned http client closed")));
    std::vector<bbt::coroutine::sync::CoWaiter::SPtr> woken;
    bool published = false;
    {
        std::lock_guard<std::mutex> lock(mutex);
        http_closed = true;
        published = MarkTerminalLocked(&woken);
    }
    if (published) {
        WakeAll(woken);
    }
}

void Client::State::WakeAll(const std::vector<bbt::coroutine::sync::CoWaiter::SPtr>& woken) noexcept
{
    idle.notify_all();
    for (const auto& waiter : woken) {
        waiter->Notify();
    }
}

// clang-format off
}
// clang-format on
