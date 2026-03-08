package com.iisigroup.df.kafkalogdemo;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.nio.charset.StandardCharsets;
import java.util.Scanner;

@Slf4j
@SpringBootApplication
public class KafkaLogDemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaLogDemoApplication.class, args);
        try (val scanner = new Scanner(System.in, StandardCharsets.UTF_8);) {
            while (true) {
                System.out.println("Enter a message to send to Kafka (type 'exit' to quit) : ");
                val inputText = scanner.nextLine();
                System.out.println("Input : " + inputText);
                if ("exit".equals(inputText)) {
                    System.out.println("結束~~~~");
                    break;
                }
                System.out.println("檢查 Kafka 是否收到訊息 !!!");
                log.info("For Kafka : {}", inputText);
            }
        }

    }

}
