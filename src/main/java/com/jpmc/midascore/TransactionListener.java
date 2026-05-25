package com.jpmc.midascore;

import java.net.URI;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import com.jpmc.midascore.repository.UserRepository;

@Component
public class TransactionListener {
    @Autowired
    private UserRepository userRepository;

    @Autowired
    private TransactionRecordRepository transactionRecordRepository;

    public final String url = "http://localhost:8080/incentive";

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core-group")
    public void listen(Transaction transaction) {
        System.out.println("Received transaction: " + transaction);
        UserRecord sender = userRepository.findById(transaction.getSenderId());
        UserRecord receiver = userRepository.findById(transaction.getRecipientId());

        if (sender != null && receiver != null && sender.getBalance() >= transaction.getAmount()) {
            RestTemplate restTemplate = new RestTemplate();
            Incentive incentive = restTemplate.postForObject(url, transaction, Incentive.class);
            float incentiveAmount = incentive != null ? incentive.getAmount() : 0;

            System.out.println("Received incentive amount: " + incentiveAmount);

            sender.setBalance(sender.getBalance() - transaction.getAmount());
            receiver.setBalance(receiver.getBalance() + transaction.getAmount() + incentiveAmount);
            userRepository.save(sender);
            userRepository.save(receiver);
            TransactionRecord record = new TransactionRecord(sender, receiver, transaction.getAmount(), incentiveAmount);
            transactionRecordRepository.save(record);
            System.out.println("Transaction with incentive recorded successful in db: " + record);
        } else {
            System.out.println("Transaction failed: insufficient funds for sender " + sender.getId());
        }
    }
}
