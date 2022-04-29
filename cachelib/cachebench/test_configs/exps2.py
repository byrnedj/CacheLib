
import sys
import json
import re
import subprocess

once = int(sys.argv[1])
sizes = [8192, 65536]
levels = ["leader","follower"]
ratios = [ 1, 4 ]
#otypes = ["fbobj" , "assocs"]
otypes = ["fbobj"]
pmem_path = "/mnt/pmem0/pmem"
bins = [ "cachebench_base" "cachebench_ff" "cachebench_reduced_cs" ]
trylockupdate = [ "true", "false" ]
insertFF = [ "true", "false" ]
backgroundEvictorInterval = [ 0, 1, 10 ]
backgroundEvictorKeepFree = [ 50, 500 ]
backgroundEvictorSchedule = [ "true", "false" ]
memconfig = [ 'DRAM', 'PMEM', 'HYBRID' ]


#exp2
# base DRAM, PMEM, HYBRID
# workloads => leader, follower
# wtype => fbobj
# cache size = 8GB, 64GB
# DRAM:PMEM ratios = 1 , 4
# trylockupdate => true
# ff = true, false
# bg_evictor = 0, 1, 10
# bg_evictor_keep_free 50, 500

cachelib_bin = "../../../opt/cachelib/bin/cachebench"
exp_res_file_lat = "/tmp/exp2_lat"
exp_res_file_tp = "/tmp/exp2_tp"

with open(exp_res_file_lat, 'w') as f:
    line = "size,ratio,ff,bg-int,bg-size,mem,op,percentile,value\n"
    f.write(line)
with open(exp_res_file_tp, 'w') as f:
    line = "size,ratio,ff,bg-int,bg-size,mem,variable,value\n"
    f.write(line)

for l in levels:
    for o in otypes:
        prefix = "hit_ratio/" + "graph_cache_" + l + "_" + o
        base_conf = prefix + "/config.json"
        #base_sizes = "hit_ratio/" + "graph_cache_" + l + "_" + o + "/sizes.json"
        #base_pop = "hit_ratio/" + "graph_cache_" + l + "_" + o + "/pop.json"
        print(base_conf)
        for s in sizes:
            for r in ratios:
                for ff in insertFF:
                    for bi in backgroundEvictorInterval:
                        for keepfree in backgroundEvictorKeepFree:
                            for m in memconfig:
                                factor = s/sizes[0] # default is 8GB
                                # edit params
                                if (m == "DRAM" or m == "PMEM") and r != 1:
                                    continue
                                if (bi == 0) and keepfree != 50:
                                    continue


                                conf = dict()
                                with open(base_conf, 'r') as f:
                                    conf = json.load(f)
                        
                                conf['cache_config']['htBucketPower'] = 28
                                conf['cache_config']['htLockPower'] = 28
                                
                                conf['cache_config']['tryLockUpdate'] = "true"
                                conf['cache_config']['cacheSizeMB'] = s
                                conf['cache_config']['nKeepFree'] = keepfree
                                conf['cache_config']['backgroundEvictorStrategy'] = "keep-free"
                                conf['cache_config']['backgroundEvictorIntervalMilSec'] = bi
                                conf['cache_config']['insertTopTier'] = ff

                                mtier = ""
                                if m == "DRAM":
                                    mtier = [{"ratio": 1}]
                                elif m == "PMEM":
                                    mtier = [{"ratio": 1, "file": pmem_path}]
                                elif m == "HYBRID":
                                    mtier = [{"ratio": 1}, { "ratio": r, "file": pmem_path}]
                                conf['cache_config']['memoryTiers'] = mtier
                                conf['test_config']['numKeys'] = factor*conf['test_config']['numKeys']
                                conf['test_config']['numOps'] = factor*conf['test_config']['numOps']
                                
                                exp_conf = prefix + "/config_wrkld_" + str(l) + "_size_" + str(s) + "_ratio_" + str(r) + "_ff_" + str(ff) + "_mem_" + m + "_bgi_" + str(bi) + "_keepfree_" + str(keepfree)
                                res_file = "/tmp/result_wrkld_" + str(l) + "_size_" + str(s) + "_ratio_" + str(r) + "_ff_" + str(ff) + "_mem_" + m + "_bgi_" + str(bi) + "_keepfree_" + str(keepfree)
                                with open(exp_conf, 'w') as f:
                                    json.dump(conf,f)
                                cmd = "numactl -N 0 " + str(cachelib_bin) + " --json_test_config " + exp_conf + " --report_api_latency" + " > " + res_file
                                
                                print(cmd)
                                print(res_file)

                                result = subprocess.check_output(cmd,shell=True)
                                latency = subprocess.check_output("./parse_to_csv_lat.sh " + res_file,shell=True)
                                tp = subprocess.check_output("./parse_to_csv_tp.sh " + res_file,shell=True)
                                hit_ratio = subprocess.check_output("./parse_to_csv_hr.sh " + res_file,shell=True).split(',')[1]
                                latency = latency.split('\n')
                                tp = tp.split('\n')
                                with open(exp_res_file_tp, 'ab') as f:
                                    line = str(s) + "," + str(r) + "," + str(ff) + "," + str(bi) + "," + str(keepfree) + "," + m + ",get," + str(tp[0]) + '\n'
                                    f.write(line)
                                    line = str(s) + "," + str(r) + "," + str(ff) + "," + str(bi) + "," + str(keepfree) + "," + m + ",set," + str(tp[1]) + '\n'
                                    f.write(line)
                                    line = str(s) + "," + str(r) + "," + str(ff) + "," + str(bi) + "," + str(keepfree) + "," + m + ",hr," + str(hit_ratio) + '\n'
                                    f.write(line)
                               
                                with open(exp_res_file_lat, 'ab') as f:
                                    for l in latency:
                                        p = l.split(',')
                                        if (len(p) == 3):
                                            line = str(s) + "," + str(r) + "," + str(ff) + "," + str(bi) + "," + str(keepfree) + "," + m + "," + p[0] + "," + p[1] + "," + str(p[2]) + '\n'
                                            f.write(line)
                                if (once == 1):
                                    exit(1)



