#!/www/python/bin/python
"""
$URL$
$Id$
"""
from sancho.unittest import TestScenario, parse_args, run_scenarios
from durus.utils import format_oid, u64, p64, u32, p32

tested_modules = ["durus.utils"]

class Test (TestScenario):

    def check_format_oid(self):
        self.test_val("format_oid('A'*8)", '4702111234474983745')

    def check_p64_u64(self):
        for x in range(3):
            self.test_val('len(p64(x))', 8)
            self.test_val('u64(p64(x))', x)

    def check_p32_u32(self):
        for x in range(3):
            assert len(p32(x)) == 4
            assert x == u32(p32(x))


if __name__ == "__main__":
    (scenarios, options) = parse_args()
    run_scenarios(scenarios, options)

