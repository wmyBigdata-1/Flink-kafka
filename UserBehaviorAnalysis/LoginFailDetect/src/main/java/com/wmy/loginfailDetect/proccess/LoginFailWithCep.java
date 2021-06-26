package com.wmy.loginfailDetect.proccess;

import com.wmy.loginfailDetect.beans.LoginEvent;
import com.wmy.loginfailDetect.beans.LoginFailWarning;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.net.URL;
import java.util.List;
import java.util.Map;

/**
 * ClassName:LoginFailWithCep
 * Package:com.wmy.loginfailDetect.proccess
 *
 * @date:2021/6/25 14:41
 * @author:数仓开发工程师
 * @email:2647716549@qq.com
 * @Description:
 */
public class LoginFailWithCep {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 1. 从文件中读取数据
        URL resource = LoginFailWithCep.class.getResource("/LoginLog.csv");
        DataStream<LoginEvent> loginEventStream = env.readTextFile(resource.getPath())
                .map(line -> {
                    String[] fields = line.split(",");
                    return new LoginEvent(new Long(fields[0]), fields[1], fields[2], new Long(fields[3]));
                })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LoginEvent>(Time.seconds(3)) {
                    @Override
                    public long extractTimestamp(LoginEvent element) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        // 定义一个匹配模式 --- 登录失败的模式
        // firstFail -> SecondFail, within 2 seconds
        Pattern<LoginEvent, LoginEvent> loginFailPattern = Pattern
                .<LoginEvent>begin("firstFail") // 只是给定一个名称而已
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return "fail".equals(value.getLoginState()); // 筛选登录状态等于fail，和filter类似
                    }
                })
                // 因为中间，不能有成功的事件
                .next("secondFail")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return "fail".equals(value.getLoginState());
                    }
                })
                // 事件限制，类似于手动开一个窗口
                .within(Time.seconds(2));

        // 将匹配模式应用导数据流上，得到一个pattern stream
        // 不是针对所有的数据进行检测，类似于两条流做连接的操作
        PatternStream<LoginEvent> patternStream = CEP.pattern(
                loginEventStream
                        .keyBy(LoginEvent::getUserId),
                loginFailPattern);

        // 检出符合条件的复杂时间，进行转换处理，得到报警信息map CoMap
        // 检测到的是一组事件，select 提取出来的都是LoginEvent，保存到Map数据结构中，key --- firstFail secondFail
        // 每组都有很多个事件，最后返回的是
        SingleOutputStreamOperator<LoginFailWarning> warningStream = patternStream
                .select(new LoginFailMatchDetectWarning());

        warningStream.print();

        env.execute("login fail detect with cep job");
    }

    // 实现自定义的PatternSelectFunction接口
    public static class LoginFailMatchDetectWarning implements PatternSelectFunction<LoginEvent,LoginFailWarning> {
        @Override
        public LoginFailWarning select(Map<String, List<LoginEvent>> pattern) throws Exception {
            LoginEvent firstFailEvent = pattern.get("firstFail").get(0); // 就是之前定义的模式匹配的内容，也可以当成一个迭代器的内容
            LoginEvent secondFailEvent = pattern.get("secondFail").get(0);
            return new LoginFailWarning(firstFailEvent.getUserId(), firstFailEvent.getTimestamp(), secondFailEvent.getTimestamp(), "login fail 2 times");
        }
    }
}
