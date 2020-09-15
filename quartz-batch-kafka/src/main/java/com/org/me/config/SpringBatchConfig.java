package com.org.me.config;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;

import com.org.me.model.User;

@Configuration
@EnableBatchProcessing
public class SpringBatchConfig {

	@Value("${input.csv.file}")
	private  Resource resource;

	@Autowired
	private ItemReader<User> itemReader;

	@Autowired
	private ItemWriter<User> itemWriter;

	@Autowired
	private ItemProcessor<User, User> itemProcessor;

	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;


	@Bean
	public Job job(){
		Step step = stepBuilderFactory.get("CSV")
							.<User, User> chunk(100)
							.reader(itemReader)
							.processor(itemProcessor)
							.writer(itemWriter)
							.allowStartIfComplete(true)
							.build();

		return jobBuilderFactory.get("Load")
							.incrementer(new RunIdIncrementer())
							.start(step)
							.build();
	}
}
