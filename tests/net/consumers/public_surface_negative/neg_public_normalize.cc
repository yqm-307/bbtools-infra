// expect-error: NormalizedOptions
#include <bbt/infra/net/Options.hpp>
using namespace bbt::infra::net;

void Probe(const Options& options)
{
    NormalizedOptions out;

    /**
     * 归一化只在 detail 头
     */
    (void)NormalizeOptions(options, out);
}
