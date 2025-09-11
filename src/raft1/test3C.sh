#!/bin/bash

# 3C测试脚本 - 并行运行100次并统计通过次数

passed=0
failed=0
total=200
max_jobs=16  # 同时运行的最大测试数量

echo "开始并行运行3C测试$total次，最大并发数: $max_jobs..."

# 创建临时目录存储结果
mkdir -p test_results

# 并行运行测试的函数
run_test() {
    local i=$1
    local output_file="test_results/test_$i.txt"
    
    # 运行3C相关的测试
    go test -run 3C > "$output_file" 2>&1
    
    # 检查测试是否通过
    if grep -q "PASS" "$output_file" && ! grep -q "FAIL" "$output_file"; then
        echo "测试 $i: PASS"
        echo "PASS" > "test_results/result_$i.txt"
    else
        echo "测试 $i: FAIL"
        echo "FAIL" > "test_results/result_$i.txt"
        cp "$output_file" "fail_results/fail_log_$i.txt"
    fi
}

# 导出函数以便在子shell中使用
export -f run_test

# 并行运行所有测试
for i in $(seq 1 $total); do
    # 控制并发数量
    while [ $(jobs -r | wc -l) -ge $max_jobs ]; do
        sleep 0.1
    done
    
    echo "启动测试 $i/$total"
    run_test $i &
done

# 等待所有测试完成
wait

# 统计结果
passed=$(ls test_results/result_* 2>/dev/null | xargs grep -l "PASS" 2>/dev/null | wc -l)
failed=$(ls test_results/result_* 2>/dev/null | xargs grep -l "FAIL" 2>/dev/null | wc -l)

echo ""
echo "测试完成!"
echo "总次数: $total"
echo "通过次数: $passed"
echo "失败次数: $failed"
echo "通过率: $(echo "scale=2; $passed * 100 / $total" | bc)%"

# 清理临时文件
rm -rf test_results