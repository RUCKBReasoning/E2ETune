# 在record里将所有的best_config转成dict

cd post_process
# 改path1 path2
python post_process.py #处理成中间格式
# 修改：将inner metrics改为读文件得到

cd ..
python fetch_plan.py #explain得到查询计划

cd post_process
python bin_data.py #训练集格式（这个是olap的文件