package com.hnust.kafkastreaming;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

public class LogProcessor implements Processor<byte[], byte[]>{

    private ProcessorContext processorContext;

    @Override
    public void init(ProcessorContext processorContext) {
        this.processorContext = processorContext;
    }

    @Override
    /**
     * line 表示读取到的一行
     */
    public void process(byte[] bytes, byte[] line) {
        String input = new String(line);

        //如果input以 PRODUCT_RATING_PREFIX:开头,表示评分数据
        if (input.contains("PRODUCT_RATING_PREFIX:")){
            System.out.println("product rating coming!!!!" + input);
            //提取出具体评分数据
            input = input.split("PRODUCT_RATING_PREFIX:")[1].trim();
            processorContext.forward("logProcessor".getBytes(), input.getBytes());
        }
    }

    @Override
    public void punctuate(long l) {

    }

    @Override
    public void close() {

    }
}
