#!/bin/bash

# Newline separated list of tests to ignore
BLACKLIST="allocator-test-AllocatorTypeTest
allocator-test-NavySetupTest
shm-test-test_page_size"

if [ "$1" == "long" ]; then
    find -type f -executable | grep -vF "$BLACKLIST" | xargs -n1 bash -c
else
    find -type f \( -not -name "*bench*" -and -not -name "navy*" \) -executable | grep -vF "$BLACKLIST" | xargs -n1 bash -c
fi
# ./allocator-test-AllocatorTypeTest --gtest_filter=-*ChainedItemSerialization*:*RebalancingWithEvictions*
