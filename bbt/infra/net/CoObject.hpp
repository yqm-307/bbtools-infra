#pragma once

#include <string>

#include <bbt/infra/net/ICoObject.hpp>

namespace bbt::infra::net {

class CoObject : public ICoObject {
public:
    // 上游 runtime 未初始化时抛 logic_error；id 沿用上游进程内唯一身份。
    explicit                    CoObject(std::string kind, std::string name = {});
    CoObject(const CoObject&) = delete;
    CoObject& operator=(const CoObject&) = delete;
    bbt::coroutine::CoObjectInfo GetObjectInfo() const override;

private:
    const bbt::coroutine::CoObjectInfo m_info;
};

} // namespace bbt::infra::net
