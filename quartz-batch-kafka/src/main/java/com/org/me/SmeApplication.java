package com.org.me;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.PropertySource;

@SpringBootApplication
@PropertySource("classpath:kafka.properties")
public class SmeApplication {

	public static void main(String[] args) {
		SpringApplication.run(SmeApplication.class, args);
	}

}
