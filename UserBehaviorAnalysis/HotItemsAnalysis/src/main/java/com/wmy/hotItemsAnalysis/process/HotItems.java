package com.wmy.hotItemsAnalysis.process;

import com.wmy.hotItemsAnalysis.beans.ItemViewCount;
import com.wmy.hotItemsAnalysis.beans.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.curator4.org.apache.curator.shaded.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Properties;

/**
 * ClassName:HotItems
 * Package:com.wmy.hotItemsAnalysis.process
 *
 * @date:2021/6/24 20:07
 * @author:数仓开发工程师
 * @email:2647716549@qq.com
 * @Description: 热门商品处理 --- 统计近一小时内的热门商品，每5分钟更新一次
 */
public class HotItems {
    public static void main(String[] args) throws Exception {
        // TODO 1、创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime); // 设置事件时间

        // TODO 2、读取数据、创建DataStream
        //URL resource = HotItems.class.getResource("/UserBehavior.csv");
        //DataStream<String> inputStream = env.readTextFile(resource.getPath());

        // 切换Kafka流数据
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "yaxin01:9092");
        properties.setProperty("group.id", "consumer");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        DataStream<String> inputStream = env.addSource(new FlinkKafkaConsumer<String>("hotitems", new SimpleStringSchema(), properties));


        // TODO 3、转换为POJO，分配时间戳和Watermark
        DataStream<UserBehavior> dataStream = inputStream.map(
                line -> {
                    String[] fields = line.split(",");
                    return new UserBehavior(new Long(fields[0]), new Long(fields[1]), new Integer(fields[2]), fields[3], new Long(fields[4]));
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(UserBehavior element) {
                        return element.getTimestamp() * 1000L; // 10位是秒
                    }
                });

        // TODO 4、分组开窗聚合，得到每个窗口内各个商品的count值
        DataStream<ItemViewCount> windowAggStream = dataStream
                .filter(data -> "pv".equals(data.getBehavior()))    // 过滤pv行为
                .keyBy("itemId")    // 按商品ID分组
                .timeWindow(Time.hours(1), Time.minutes(5))    // 开滑窗
                .aggregate(new ItemCountAgg(), new WindowItemCountResult()); // 增量聚合函数和全窗口函数

        // TODO 5. 收集同一窗口的所有商品count数据，排序输出top n
        // 格式化处理
        DataStream<String> resultStream = windowAggStream
                .keyBy("windowEnd")    // 按照窗口分组
                .process(new TopNHotItems(5));   // 用自定义处理函数排序取前5

        resultStream.print();

        env.execute("hot items analysis"); // 生产环境当中都要给定一个名字，要不然不好排查信息
    }

    // TODO 实现自定义增量聚合函数 --- 预聚合操作
    public static class ItemCountAgg implements AggregateFunction<UserBehavior, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    // TODO 自定义全窗口函数

    /**
     * <IN> The type of the input value.
     * <OUT> The type of the output value.
     * <KEY> The type of the key.
     * <W> The type of {@code Window} that this window function can be applied on.
     */
    public static class WindowItemCountResult implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow> {
        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Long> input, Collector<ItemViewCount> out) throws Exception {
            Long itemId = tuple.getField(0); // 一元组
            Long windowEnd = window.getEnd(); // 本身就是一个长整型
            Long count = input.iterator().next(); // 本来是存的全量数据，现在就是一个值那就是次数
            out.collect(new ItemViewCount(itemId, windowEnd, count)); // WindowFunction是没有返回值结果
        }
    }

    // TODO实现自定义KeyedProcessFunction

    /**
     * Type of the key. --- 当前键的类型
     * Type of the input elements. --- 当前的输入
     * Type of the output elements. --- 格式化的类型
     */
    public static class TopNHotItems extends KeyedProcessFunction<Tuple, ItemViewCount, String> {
        // 定义属性，top n的大小
        private Integer topSize;

        // 如果没有这个的话会报错
        public TopNHotItems(Integer topSize) {
            this.topSize = topSize;
        }

        // 定义列表状态，保存当前窗口内所有输出的ItemViewCount
        ListState<ItemViewCount> itemViewCountListState;

        // 拿到当前的getRuntimeContext，这个有很多数据结构类型
        @Override
        public void open(Configuration parameters) throws Exception {
            itemViewCountListState = getRuntimeContext().getListState(new ListStateDescriptor<ItemViewCount>("item-view-count-list", ItemViewCount.class));
        }

        @Override
        public void processElement(ItemViewCount value, Context ctx, Collector<String> out) throws Exception {
            // 每来一条数据，存入List中，并注册定时器
            itemViewCountListState.add(value);
            ctx.timerService().registerEventTimeTimer( value.getWindowEnd() + 1 ); // 这样会不会注册很多定时器，同一个窗口的定时器都是一个在一个窗口内
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定时器触发，当前已收集到所有数据，排序输出
            ArrayList<ItemViewCount> itemViewCounts = Lists.newArrayList(itemViewCountListState.get().iterator()); // 里面是可以传入一个迭代器的类型，要看get返回的类型是什么

            itemViewCounts.sort(new Comparator<ItemViewCount>() {
                @Override
                public int compare(ItemViewCount o1, ItemViewCount o2) {
                    return (o2.getCount().intValue() - o1.getCount().intValue()); // 降序排列
                }
            });

            // 将排名信息格式化成String，方便打印输出
            StringBuilder resultBuilder = new StringBuilder();
            resultBuilder.append("===================================\n");
            resultBuilder.append("窗口结束时间：").append( new Timestamp(timestamp - 1)).append("\n");

            // 遍历列表，取top n输出
            for( int i = 0; i < Math.min(topSize, itemViewCounts.size()); i++ ){
                ItemViewCount currentItemViewCount = itemViewCounts.get(i);
                resultBuilder.append("NO ").append(i+1).append(":")
                        .append(" 商品ID = ").append(currentItemViewCount.getItemId())
                        .append(" 热门度 = ").append(currentItemViewCount.getCount())
                        .append("\n");
            }
            resultBuilder.append("===============================\n\n");

            // 控制输出频率
            Thread.sleep(1000L);

            out.collect(resultBuilder.toString());
        }
    }
}
