package com.wmy.orderPay.process;

import com.wmy.orderPay.beans.OrderEvent;
import com.wmy.orderPay.beans.OrderResult;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.net.URL;

/**
 * ClassName:OrderTimeOutOnProcess
 * Package:com.wmy.orderPay.process
 *
 * @date:2021/6/25 18:02
 * @author:数仓开发工程师
 * @email:2647716549@qq.com
 * @Description: 判断订单超时事件，使用ProcessFuncton
 */
public class OrderTimeOutOnProcess {
    // 定义超时事件的侧输出流标签
    private final static OutputTag<OrderResult> orderTimeoutTag = new OutputTag<OrderResult>("order-timeout"){};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        // 读取数据并转换成POJO类型
        URL resource = OrderTimeOutOnProcess.class.getResource("/OrderLog.csv");
        DataStream<OrderEvent> orderEventStream = env.readTextFile(resource.getPath())
                .map(line -> {
                    String[] fields = line.split(",");
                    return new OrderEvent(new Long(fields[0]), fields[1], fields[2], new Long(fields[3]));
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderEvent>() {
                    @Override
                    public long extractAscendingTimestamp(OrderEvent element) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        // 统一就一步操作，如果到点Pay还没有来的话，状态，定时器，process
        // 自定义处理函数，定义主流输出正常匹配订单事件，侧输出流超时报警事件
        SingleOutputStreamOperator<OrderResult> resultStream = orderEventStream
                .keyBy(OrderEvent::getOrderId)
                .process(new OrderPayMatchDetect());

        resultStream.print();
        resultStream.getSideOutput(orderTimeoutTag).print();
        env.execute("order timeout without cep job");
    }

    // 实现自定义的KeyedProcessFunction
    public static class OrderPayMatchDetect extends KeyedProcessFunction<Long, OrderEvent, OrderResult> {
        // 定义状态，保存之前订单是否已经来过create、pay的事件
        ValueState<Boolean> isPayedState;
        ValueState<Boolean> isCreateState;

        // 定义状态，保存定时器时间戳
        ValueState<Long> timerTsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 这个名称必须是不一样的
            isPayedState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("is-payed", Boolean.class, false));
            isCreateState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("is-created", Boolean.class, false));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-ts", Long.class));
        }

        @Override
        public void processElement(OrderEvent value, Context ctx, Collector<OrderResult> out) throws Exception {
            // 先获取当前的状态
            Boolean isPayed = isPayedState.value();
            Boolean isCreated = isCreateState.value();
            Long timerTs = timerTsState.value();

            // 判断当前事件类型
            if ("create".equals(value.getEventType())) {
                // 如果来的是create，要判断是否支付，乱序
                if (isPayed) {
                    // 如果已经正常支付，乱序，肯定离得很近，输出正常匹配结果
                    out.collect(new OrderResult(value.getOrderId(), "payed successfully 。。。"));
                    // 清空状态，删除定时器
                    isCreateState.clear(); // 这个应该是没有得
                    isPayedState.clear();
                    timerTsState.clear();
                    ctx.timerService().deleteEventTimeTimer(timerTs); // pay事件来了要不要也要注册一个定时器，无限等下去
                } else {
                    // 如果要是没有支付过，这个是正常得，注册15分钟得定时器，开始等待支付事件
                    Long ts = (value.getTimestamp() + 15 * 60) * 1000L;
                    ctx.timerService().registerEventTimeTimer(ts);
                    // 更新状态
                    timerTsState.update(ts);
                    isCreateState.update(true);
                }
            } else if ("pay".equals(value.getEventType())) {
                // 如果是pay事件，要判断是否 有下单事件来过
                if (isCreated) {
                    // 已经有过下单事件，继续判断当前得支付得时间戳，是否超过15分钟
                    if (value.getTimestamp() * 1000L < timerTs) {
                        // 这个在15分钟内，没有超时，正常匹配
                        out.collect(new OrderResult(value.getOrderId(), "payed successfully 。。。"));
                    } else {
                        // 已经超时，输出侧输出流报警
                        ctx.output(orderTimeoutTag, new OrderResult(value.getOrderId(), "payed but already timeout 。。。"));
                    }
                    // 统一清空状态，删除定时器
                    isCreateState.clear();
                    isPayedState.clear();
                    timerTsState.clear();
                    ctx.timerService().deleteEventTimeTimer(timerTs);
                } else {
                    // 没有下单事件，肯定是一个乱序，注册一个定时器，等待下单事件得到来
                    ctx.timerService().registerEventTimeTimer(value.getTimestamp() * 1000L); // 支付事件到来，肯定是下单事件，直接利用watermark得延迟，当前时间戳，不代表马上要触发
                    timerTsState.update(value.getTimestamp() * 1000L);
                    isPayedState.update(true);
                }
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<OrderResult> out) throws Exception {
            // 定时器触发，说明有一个事件没有来，如果都没有来，不可能有事件
            if (isPayedState.value()) {
                // 如果pay来了，说明create没有来，直接做一个报警
                ctx.output(orderTimeoutTag, new OrderResult(ctx.getCurrentKey(), "payed but not fount create event 。。。"));
            } else {
                // 如果pay没来
                ctx.output(orderTimeoutTag, new OrderResult(ctx.getCurrentKey(), "timout 。。。"));
            }

            // 清空状态
            isCreateState.clear();
            isPayedState.clear();
            timerTsState.clear();
        }
    }
}
