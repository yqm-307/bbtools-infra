#pragma once

/**
 * @file Types.hpp
 * @brief 公共网络值类型：读状态与读结果、写阶段与写结果、写方向接纳状态。
 *
 * 这些值的语义与成员含义属于公共契约。它们不放在 detail 中，是为了让公共头不必间接暴露
 * 接收环的布局或内部枚举的位置；内部存储、通知与游标策略不属于契约，调用方不应依赖。
 */
#include <cstddef>
#include <system_error>

namespace bbt::infra::net {

/**
 * @brief 读取操作的状态。
 *
 * - Ok：已取得数据保护，可以访问数据。
 * - Busy：与其它路径竞争，零消费；释放后需要重试。
 * - WouldBlock：暂无数据，且尚未到达终态。
 * - Eof、Error：持久终态，之后不再有数据。Eof 只在尾部数据耗尽后出现。
 */
enum class ReadStatus { Ok, Busy, WouldBlock, Eof, Error };

/**
 * @brief TryRead 的结果。
 */
struct ReadResult {

    /**
     * 结果状态
     */
    ReadStatus status{ReadStatus::WouldBlock};

    /**
     * Ok 时本次复制并消费的字节数
     */
    std::size_t bytes{0};

    /**
     * 持久错误原因，仅 Error 状态有意义
     */
    std::error_code error;
};

/**
 * @brief 写操作所处的阶段。
 *
 * - InFlight：已接纳，仍在途，尚未到达终态。
 * - Done：已到达终态。是否成功要看 error 是否为空；Done 且 error 为空只表示本地完成，
 *   并不代表对端已收到或处理。
 * - Rejected：未提交，没有任何副作用。
 * - Unknown：只来自无效写句柄的 Query，没有可识别的身份。
 */
enum class WritePhase { InFlight, Done, Rejected, Unknown };

/**
 * @brief 写操作在某一时刻的观察结果。
 *
 * 这是采样值，表示返回那一刻的状态；调用返回时操作未必仍在途。
 */
struct WriteOutcome {

    /**
     * 采样时的阶段
     */
    WritePhase phase{WritePhase::Unknown};

    /**
     * 该操作实际交给内核的字节前缀
     */
    std::size_t sent{0};

    /**
     * 该操作的真实错误
     */
    std::error_code error;
};

/**
 * @brief 连接写方向的接纳状态。
 *
 * - Open：可以接纳新写。
 * - Settling：关闭或写失败的结算进行中，新写被拒绝。
 * - Closed：终态。
 */
enum class AcceptanceState { Open, Settling, Closed };

// clang-format off
}
// clang-format on
