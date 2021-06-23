package com.karl.one.consumer;

import com.karl.one.Company;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * @author karl xie
 */
@Slf4j
public class CompanyDeserializer implements Deserializer<Company> {

    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    public Company deserialize(String topic, byte[] data) {
        try {
            if (data == null) {
                return null;
            }
            if (data.length < 8) {
                throw new SerializationException("Size of data received " +
                        "by DemoDeserializer is shorter than expected!");
            }
            ByteBuffer buffer = ByteBuffer.wrap(data);
            int nameLen, addressLen;
            String name, address;

            nameLen = buffer.getInt();
            byte[] nameBytes = new byte[nameLen];
            buffer.get(nameBytes);
            addressLen = buffer.getInt();
            byte[] addressBytes = new byte[addressLen];
            buffer.get(addressBytes);

            try {
                name = new String(nameBytes, "UTF-8");
                address = new String(addressBytes, "UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new SerializationException("Error occur when deserializing!");
            }

            return new Company(name, address);
        } catch (Exception e) {
            log.error("消费失败", e);
        }
        return new Company();
    }

    public void close() {
    }
}