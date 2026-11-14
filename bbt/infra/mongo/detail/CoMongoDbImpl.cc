#include <bbt/infra/mongo/detail/CoMongoDbImpl.hpp>
#include <bbt/infra/mongo/detail/MongoCollImpl.hpp>
#include <bbt/infra/detail/CloseWait.hpp>

namespace bbt::infra::mongo_detail {
CoMongoDbImpl::CoMongoDbImpl(std::shared_ptr<MongoRuntime> runtime, bbt::coroutine::CoObjectInfo info)
    : m_runtime(std::move(runtime))
    , m_info(std::move(info))
{
}

CoMongoDbImpl::~CoMongoDbImpl()
{
    (void)Close(CloseOptions{});
}

result<void> CoMongoDbImpl::Start()
{
    return m_runtime->Start();
}

result<std::shared_ptr<mongo::CoMongoColl>> CoMongoDbImpl::Collection(mongo::MongoTarget target)
{
    auto valid = mongo::ValidateMongoTarget(target);
    if (!valid)
        return result<std::shared_ptr<mongo::CoMongoColl>>::err(std::move(valid).error());
    if (!m_close.IsOpen())
        return result<std::shared_ptr<mongo::CoMongoColl>>::err(
            MakeError(ErrorCode::Closed, "mongo owner is closing or closed"));

    /**
     * 公共契约：owner 必须 Running 才接纳新句柄（Client.hpp）。
     */
    if (!m_runtime->IsRunning())
        return result<std::shared_ptr<mongo::CoMongoColl>>::err(
            MakeError(ErrorCode::RuntimeUnavailable, "mongo owner not started"));
    auto info = NewObjectInfo("infra.mongo_coll");
    if (!info)
        return result<std::shared_ptr<mongo::CoMongoColl>>::err(std::move(info).error());
    auto h = std::make_shared<MongoCollImpl>(m_runtime, std::move(target), std::move(info).value());
    return result<std::shared_ptr<mongo::CoMongoColl>>::ok(std::move(h));
}

result<void> CoMongoDbImpl::Close(const CloseOptions& options)
{
    auto valid = bbt::infra::detail::ValidateCloseOptions(options);
    if (!valid)
        return valid;
    if ((options.mode == CloseMode::Graceful || options.wait_release)
        && bbt::infra::detail::CurrentCloseWaitDomain() == bbt::infra::detail::CloseWaitDomain::Invalid)
        return result<void>::err(
            MakeError(ErrorCode::InvalidContext, "mongo: Close requires coroutine or native host"));
    m_close.BeginClose();
    return m_runtime->Close(options);
}

std::optional<Error> CoMongoDbImpl::CloseReason() const noexcept
{
    return m_runtime->CloseReason();
}

bool CoMongoDbImpl::IsClosed() const noexcept
{
    return m_runtime->IsClosed();
}

bbt::coroutine::CoObjectInfo CoMongoDbImpl::GetObjectInfo() const
{
    return m_info;
}

std::shared_ptr<MongoRuntime> CoMongoDbImpl::Runtime() const
{
    return m_runtime;
}

// clang-format off
}
// clang-format on
