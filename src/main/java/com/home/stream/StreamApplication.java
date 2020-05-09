package com.home.stream;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;

@SpringBootApplication
@EnableBinding(CustomKafkaStreamsProcessor.class)
public class StreamApplication {

	@Component
	public static class PageViewEventSource implements ApplicationRunner {
		private final MessageChannel pageViewOut;

		public PageViewEventSource(CustomKafkaStreamsProcessor binding) {
			this.pageViewOut = binding.pageViewOut();
		}

		@Override
		public void run(ApplicationArguments args) throws Exception {
			// TODO Auto-generated method stub
			List<String> names = Arrays.asList("satya", "Tarun", "swapna", "lohita");
			List<String> pages = Arrays.asList("About", "careers", "products", "tickets");
			Runnable runnable = () -> {
				String rPage = pages.get(new Random().nextInt(pages.size()));
				String rName = pages.get(new Random().nextInt(pages.size()));
				PageViewEvent pageViewEvent = new PageViewEvent(rName, rPage, Math.random() > .5 ? 10 : 1000);
				Message<PageViewEvent> message = MessageBuilder.withPayload(pageViewEvent)
						.setHeader(KafkaHeaders.MESSAGE_KEY, pageViewEvent.getUserId().getBytes()).build();
				try {
					this.pageViewOut.send(message);
					System.out.println("sent , " + message.toString());
				} catch (Exception e) {
					System.out.println(e);
				}
			};
			Executors.newScheduledThreadPool(1).scheduleAtFixedRate(runnable, 1, 1, TimeUnit.SECONDS);
		}

	}

	@Component
	public static class PageViewEventProcessor {
		@StreamListener
		@SendTo(CustomKafkaStreamsProcessor.PAGE_COUNT_OUT)
		public KStream<String, Long> process(
				@Input(CustomKafkaStreamsProcessor.PAGE_VIEW_IN) KStream<String, PageViewEvent> events) {
			return events.filter((key, value) -> value.getDuration() > 10)
					.map((key, value) -> new KeyValue<>(value.getPage(), "0")).groupByKey()
					.count(Materialized.as(CustomKafkaStreamsProcessor.PAGE_COUNT_MV)).toStream();
		}
	}

	@Component
	public static class PageCountSink {
		@StreamListener
		public void process(@Input(CustomKafkaStreamsProcessor.PAGE_COUNT_IN) KTable<String, Long> counts) {
			System.out.println("Recieving counts"+counts);
			counts.toStream().foreach((key, value) -> System.out.println("Recieved "+ key + "=" + value));
		}

	}

	public static void main(String[] args) {
		SpringApplication.run(StreamApplication.class, args);
	}

//	private void sendData() {
//		  StreamsBuilder builder = new StreamsBuilder();
//	        KStream<String, String> textLines = builder.stream("TextLinesTopic");
//	        KTable<String, Long> wordCounts = textLines
//	            .flatMapValues(textLine -> Arrays.asList(textLine.toLowerCase().split("\\W+")))
//	            .groupBy((key, word) -> word)
//	            .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts-store"));
//	        wordCounts.toStream().to("WordsWithCountsTopic", Produced.with(Serdes.String(), Serdes.Long()));
//	 
//	        KafkaStreams streams = new KafkaStreams(builder.build(), props);
//	        streams.start();
//	}

//	public static class WordCountProcessorApplication {
//
//		public static final String INPUT_TOPIC = "input";
//		public static final String OUTPUT_TOPIC = "output";
//		public static final int WINDOW_SIZE_MS = 30000;
//		
//		   @StreamListener(GreetingsStreams.INPUT)
//		    public KStream<Object, WordCount> handleGreetings(KStream<String, String> greetings) {
//		        System.out.println("Received greetings: {} , "+ greetings);
//
//				return  greetings
//						.flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
//						.map((key, value) -> new KeyValue<>(value, value))
//						.groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
//						.windowedBy(TimeWindows.of(Duration.ofMillis(WINDOW_SIZE_MS)))
//						.count(Materialized.as("WordCounts-1"))
//						.toStream()
//						.map((key, value) -> new KeyValue<>(null, new WordCount(key.key(), value, new Date(key.window().start()), new Date(key.window().end()))));
//		    }
////
//		@Bean
//		public  Function<KStream<Bytes, String>, KStream<Bytes, WordCount>> process() {
//			
//			return input -> input
//					.flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
//					.map((key, value) -> new KeyValue<>(value, value))
//					.groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
//					.windowedBy(TimeWindows.of(Duration.ofMillis(WINDOW_SIZE_MS)))
//					.count(Materialized.as("WordCounts-1"))
//					.toStream()
//					.map((key, value) -> new KeyValue<>(null, new WordCount(key.key(), value, new Date(key.window().start()), new Date(key.window().end()))));
//		}
//		
//	//	@Bean
////		@StreamListener("input")
////		  @SendTo("output")
////		  public void process(KStream<Object, String> input) {
////
////			KStream<Windowed<String>, Long> map =  input
////		           .flatMapValues(
////		              value -> Arrays.asList(value.toLowerCase().split("\\W+")))
////		           .map((key, value) -> new KeyValue<>(value, value))
////		           .groupByKey()
////		           .windowedBy(TimeWindows.of(Duration.ofMillis(WINDOW_SIZE_MS)))
////		                   .count(Materialized.as("wordcounts"))
////		           .toStream();
////			
////			System.out.println(map);
////			
////			//return new KeyValue<>(null, new WordCount(map.k, value)));
//////		           .map((key, value) -> {
//////		        	   
//////		           });
////		     //new KeyValue<>(null, new WordCount(key.key(), value)));
////		  }
//	}

	static class WordCount {

		private String word;

		private long count;

		private Date start;

		private Date end;

		@Override
		public String toString() {
			final StringBuffer sb = new StringBuffer("WordCount{");
			sb.append("word='").append(word).append('\'');
			sb.append(", count=").append(count);
			sb.append(", start=").append(start);
			sb.append(", end=").append(end);
			sb.append('}');
			return sb.toString();
		}

		WordCount() {

		}

		WordCount(String word, long count, Date start, Date end) {
			this.word = word;
			this.count = count;
			this.start = start;
			this.end = end;
		}

		public String getWord() {
			return word;
		}

		public void setWord(String word) {
			this.word = word;
		}

		public long getCount() {
			return count;
		}

		public void setCount(long count) {
			this.count = count;
		}

		public Date getStart() {
			return start;
		}

		public void setStart(Date start) {
			this.start = start;
		}

		public Date getEnd() {
			return end;
		}

		public void setEnd(Date end) {
			this.end = end;
		}
	}
}

interface CustomKafkaStreamsProcessor {
	String PAGE_COUNT_OUT = "pcout";

	String PAGE_COUNT_MV = "pcmv";
	String PAGE_VIEWS_OUT = "pvout";
	String PAGE_VIEW_IN = "pvin";
	String PAGE_COUNT_IN = "pcin";

	@Output(PAGE_COUNT_OUT)
	KStream<String, Long> pageCountOut();

	@Input(PAGE_COUNT_IN)
	KTable<String, Long> pageCountIn();

	@Output(PAGE_VIEWS_OUT)
	MessageChannel pageViewOut();

	@Input(PAGE_VIEW_IN)
	KStream<String, PageViewEvent> pageViewIn();
}


class PageViewEvent {

	private String userId, page;
	private long duration;

	PageViewEvent(){
		
	}
	public PageViewEvent(String rName, String rPage, int i) {
		// TODO Auto-generated constructor stub
		this.userId = rName;
		this.page = rPage;
		this.duration = i;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public String getPage() {
		return page;
	}

	public void setPage(String page) {
		this.page = page;
	}

	public long getDuration() {
		return duration;
	}

	public void setDuration(long duration) {
		this.duration = duration;
	}

	@Override
	public String toString() {
		return "PageViewEvent [userId=" + userId + ", page=" + page + ", duration=" + duration + "]";
	}

}
