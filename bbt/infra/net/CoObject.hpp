#pragma once

/**
 * @file CoObject.hpp
 * @brief net 中登记了上游身份的协程对象基类。
 */

#include <string>

#include <bbt/infra/net/ICoObject.hpp>

namespace bbt::infra::net {

/**
 * @brief 带 kind 与 name 身份信息的协程对象。
 *
 * 身份 id 沿用上游进程内唯一的编号。拷贝被禁用：一个对象只对应一个上游身份。
 */
class CoObject : public ICoObject {
public:
    /**
     * @brief 构造对象并向上游登记身份。
     *
     * 上游 runtime 未初始化时，CreateObjectInfo 抛出 std::logic_error。
     *
     * @param kind 对象类别，例如 "infra.net.Engine"。
     * @param name 实例名，缺省为空。
     */
    explicit CoObject(std::string kind, std::string name = {});
    CoObject(const CoObject&) = delete;
    CoObject& operator=(const CoObject&) = delete;

    /** @brief 返回上游登记的对象身份信息。 */
    bbt::coroutine::CoObjectInfo GetObjectInfo() const override;

private:
    const bbt::coroutine::CoObjectInfo m_info;
};

} // namespace bbt::infra::net
