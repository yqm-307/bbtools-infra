#!/usr/bin/env python3
"""核对 network-only CTest 注册集与 JUnit 实跑集，缺项/重复/跳过均失败。"""
import json
import sys
import xml.etree.ElementTree as ET


REQUIRED = {
    'example_co_net_service_roundtrip', 'example_ico_connection_roundtrip',
    'net.consumer',
    'net.consumer_coroutine_first_configure', 'net.consumer_coroutine_first_build',
    'net.consumer_coroutine_first_run', 'net.consumer_source_dir_mismatch',
    'net.udp_high_level_compile_reject', 'net.buffer_allocation',
    'net.reactor_driver_bind', 'net.reactor_driver_failed',
    'net.r2_raw_roundtrip', 'net.r2_co_accept_read_write', 'net.r2_co_dial',
    'net.b4_co_pingpong_strict', 'net.b4_request_close_write_error_intent',
    'net.b4_request_close_write_readiness', 'net.b4_sealed_write_error_read_publication',
}


def check(registration, report):
    names = [test['name'] for test in registration['tests']]
    expected = set(names)
    # R10 基线为 111 项；新增可自动纳入，删除须显式评审此覆盖下限。
    if len(names) < 111 or len(names) != len(expected) or not REQUIRED <= expected:
        raise ValueError('network-only 注册集缺项或重复（至少111项且关键入口齐全）')
    if any(not (name.startswith('net.') or name.startswith('example_'))
           for name in names):
        raise ValueError('network-only 混入非网络测试')
    cases = list(report.iter('testcase'))
    if len(cases) != len(names) or {case.get('name') for case in cases} != expected:
        raise ValueError('network-only JUnit 实跑集与注册集不一致或重复')
    if any(case.find(tag) is not None for case in cases
           for tag in ('skipped', 'failure', 'error')):
        raise ValueError('network-only 存在跳过、失败或错误')
    print(f'network-only: {len(cases)} passed, 0 failed, 0 skipped')


if __name__ == '__main__':
    try:
        with open(sys.argv[1], encoding='utf-8') as source:
            registration = json.load(source)
        check(registration, ET.parse(sys.argv[2]).getroot())
    except (OSError, ValueError, KeyError, ET.ParseError) as exc:
        raise SystemExit(f'FAIL: {exc}') from exc
