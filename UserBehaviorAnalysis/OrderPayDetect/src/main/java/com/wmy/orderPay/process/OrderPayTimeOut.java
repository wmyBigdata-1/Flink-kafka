package com.wmy.orderPay.process;

import com.wmy.orderPay.beans.OrderEvent;
import com.wmy.orderPay.beans.OrderResult;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.net.URL;
import java.util.List;
import java.util.Map;

/**
 * ClassName:OrderPayTimeOut
 * Package:com.wmy.orderPay.process
 *
 * @date:2021/6/25 17:25
 * @author:数仓开发工程师
 * @email:2647716549@qq.com
 * @Description: 订单超时事件
 */
public class OrderPayTimeOut {
    public static void main(String[] args) throws Exception {
        // 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        // 读取数据并转换为POJO类型
        URL resource = OrderPayTimeOut.class.getResource("/OrderLog.csv");
        DataStream<OrderEvent> orderEventStream = env.readTextFile(resource.getPath())
                .map(line -> {
                    String[] fields = line.split(",");
                    return new OrderEvent(new Long(fields[0]), fields[1], fields[2], new Long(fields[3]));
                })
                // 升序的方式
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderEvent>() {
                    @Override
                    public long extractAscendingTimestamp(OrderEvent element) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        // 定义一个带时间限制的模式
        // 检测的是15分钟之内必须支付，这个是成功之后的结果
        Pattern<OrderEvent, OrderEvent> orderPayPattern = Pattern.<OrderEvent>begin("create").where(new SimpleCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent value) throws Exception {
                return "create".equals(value.getEventType());
            }
        })
                // 宽松近邻：支付相关
                .followedBy("pay").where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return "pay".equals(value.getEventType());
                    }
                })
                // 订单在15分钟之内完成
                .within(Time.seconds(15));

        // 定义侧输出流标签，用来表示超时事件
        OutputTag<OrderResult> orderTimeoutTag = new OutputTag<OrderResult>("order-timeout"){}; // 记住这个一定要{}，否则会报错

        // 将pattern应用到输入数据流上，得到pattern stream
        // 并没有指定每一订单，首先先分组
        PatternStream<OrderEvent> patternStream = CEP.pattern(orderEventStream.keyBy(OrderEvent::getOrderId), orderPayPattern);

        // 调用select方法实现对匹配复杂事件和超时复杂事件的提取和处理
        SingleOutputStreamOperator<OrderResult> resultStream = patternStream.select(
                orderTimeoutTag,
                new OrderTimeoutSelect(),
                new OrderPaySelect());

        // 正常支付的结果流
        resultStream.print();

        // 超时的事件流
        resultStream.getSideOutput(orderTimeoutTag).print("timeout");

        env.execute("order timeout detect job");

    }

    // 实现自定义的超时事件处理函数
    // 多了一个处理超时事件的处理
    public static class OrderTimeoutSelect implements PatternTimeoutFunction<OrderEvent,OrderResult> {
        // 拿到订单的ID就可以，获取一个长整型的订单ID
        @Override
        public OrderResult timeout(Map<String, List<OrderEvent>> pattern, long timeoutTimestamp) throws Exception {
            Long timeoutOrderId = pattern.get("create").iterator().next().getOrderId(); // 单列模式
            return new OrderResult(timeoutOrderId, "timeout ---> " + timeoutTimestamp);
        }
    }


    // 实现自定义的正常匹配事件处理函数
    public static class OrderPaySelect implements PatternSelectFunction<OrderEvent, OrderResult> {
        @Override
        public OrderResult select(Map<String, List<OrderEvent>> pattern) throws Exception {
            Long payedOrderId = pattern.get("pay").iterator().next().getOrderId(); // 单列模式
            return new OrderResult(payedOrderId, "payed ---> ");
        }
    }
}

