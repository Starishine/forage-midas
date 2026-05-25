package com.jpmc.midascore.entity;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;

@Entity
public class TransactionRecord {
    @Id
    @GeneratedValue()
    private long id;

    @JoinColumn(name = "sender_id", nullable = false)
    @ManyToOne
    private UserRecord sender;

    @JoinColumn(name = "recipient_id", nullable = false)
    @ManyToOne
    private UserRecord recipient;

    private float incentive;
    private float amount;

    public TransactionRecord() {}
    public TransactionRecord(UserRecord sender, UserRecord recipient, float amount, float incentive) {
        this.sender = sender;
        this.recipient = recipient;
        this.amount = amount;
        this.incentive = incentive;
    }

    public long getId() {
        return id;
    }

    public UserRecord getSender() {
        return sender;
    }
    public UserRecord getRecipient() {
        return recipient;
    }

    public float getAmount() {
        return amount;
    }

    public float getIncentive() {
        return incentive;
    }
}
