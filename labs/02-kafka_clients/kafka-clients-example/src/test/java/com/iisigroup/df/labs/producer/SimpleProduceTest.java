package com.iisigroup.df.labs.producer;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static com.iisigroup.df.labs.constant.Constants.TEST_TOPIC;
import static com.iisigroup.df.labs.constant.Constants.VALUE_PREFIX;

/**
 * Kafka Producer 基本發送模式示範。
 * <p>
 * 本測試類別展示三種常見的訊息發送方式：
 * <ul>
 *     <li><b>非同步發送（Fire-and-Forget）</b>— 呼叫 {@code send()} 後不等待結果</li>
 *     <li><b>非同步發送 + Callback</b>— 透過回呼函式接收發送結果或例外</li>
 *     <li><b>同步發送</b>— 呼叫 {@code send().get()} 阻塞等待 Broker 回應</li>
 * </ul>
 * </p>
 *
 * @see org.apache.kafka.clients.producer.KafkaProducer
 */
@Slf4j
public class SimpleProduceTest {

    /**
     * 非同步發送（Fire-and-Forget）。
     * <p>
     * 呼叫 {@code send()} 後立即返回，不等待 Broker 的確認回應。
     * 這是最高吞吐量的發送方式，但無法得知訊息是否成功抵達 Broker。
     * 若發送失敗，Producer 內部會自動重試（依預設 retries 設定），
     * 但最終失敗時不會有任何通知。
     * </p>
     */
    @Test
    public void asyncProduce() {
        val properties = new Properties();

        // Kafka 叢集的連線位址（host:port），Producer 會透過這個位址找到整個叢集
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        // 訊息的 key 要用什麼方式轉成 byte[]，這裡用字串序列化器
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 訊息的 value 要用什麼方式轉成 byte[]，這裡用字串序列化器
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // try-with-resources 確保 Producer 正常關閉，flush 殘餘訊息
        try (val kafkaProducer = new KafkaProducer<String, String>(properties)) {
            for (int i = 0; i < 5; i++) {
                // 不指定 key，由 DefaultPartitioner 以 Sticky Partitioning 策略分配 partition
                kafkaProducer.send(new ProducerRecord<>(TEST_TOPIC, VALUE_PREFIX + i));
            }
        }

    }

    /**
     * 非同步發送 + Callback。
     * <p>
     * 透過 {@link org.apache.kafka.clients.producer.Callback} 取得每筆訊息的發送結果。
     * 當訊息成功寫入 Broker 後，回呼函式會收到 {@link org.apache.kafka.clients.producer.RecordMetadata}，
     * 包含 offset、partition、timestamp 等資訊；
     * 若發送失敗，則 {@code exception} 參數不為 null，可在此處理錯誤邏輯。
     * </p>
     * <p>
     * <b>注意：</b>Callback 是在 Producer 的 I/O 執行緒中執行，
     * 應避免在 Callback 中做耗時操作，以免影響整體吞吐量。
     * </p>
     */
    @Test
    public void asyncProduceWithCallback() {
        val properties = new Properties();

        // Kafka 叢集的連線位址（host:port），Producer 會透過這個位址找到整個叢集
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        // 訊息的 key 要用什麼方式轉成 byte[]，這裡用字串序列化器
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 訊息的 value 要用什麼方式轉成 byte[]，這裡用字串序列化器
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (val kafkaProducer = new KafkaProducer<String, String>(properties)) {
            for (int i = 0; i < 5; i++) {
                kafkaProducer.send(
                        new ProducerRecord<>(TEST_TOPIC, VALUE_PREFIX + i)
                        , (metadata, exception) -> {
                            if (exception != null) {
                                // 發送失敗：可能因網路中斷、序列化錯誤或超過重試次數
                                log.error("send message error", exception);
                                return;
                            }
                            // 發送成功：印出寫入的 offset、partition、timestamp、topic
                            val offset = metadata.offset();
                            val partition = metadata.partition();
                            val timestamp = metadata.timestamp();
                            val topic = metadata.topic();
                            log.info("offset: {}, partition: {}, timestamp: {}, topic: {}", offset, partition, timestamp, topic);
                        });
            }
        }

    }


    /**
     * 同步發送（Synchronous Send）。
     * <p>
     * 呼叫 {@code send().get()} 會阻塞當前執行緒，直到 Broker 回傳確認或拋出例外。
     * 這是最可靠但吞吐量最低的方式，適合對資料正確性要求極高的場景。
     * </p>
     *
     * @throws ExecutionException   當 Broker 回傳錯誤時拋出
     * @throws InterruptedException 當等待過程中執行緒被中斷時拋出
     */
    @Test
    public void syncProduce() throws ExecutionException, InterruptedException {
        val properties = new Properties();

        // Kafka 叢集的連線位址（host:port），Producer 會透過這個位址找到整個叢集
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        // 訊息的 key 要用什麼方式轉成 byte[]，這裡用字串序列化器
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 訊息的 value 要用什麼方式轉成 byte[]，這裡用字串序列化器
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (val kafkaProducer = new KafkaProducer<String, String>(properties)) {
            for (int i = 0; i < 5; i++) {
                // .get() 會阻塞直到訊息成功寫入或拋出例外
                kafkaProducer.send(new ProducerRecord<>(TEST_TOPIC, VALUE_PREFIX + i)).get();
            }
        }
    }

}
