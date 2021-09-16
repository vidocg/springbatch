package com.config;

import com.model.Account;
import com.model.Profile;
import com.model.User;
import com.processor.ProfileAccountProcessor;
import com.processor.UserProfileProcessor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.DefaultBatchConfigurer;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.MultiResourceItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.xml.StaxEventItemWriter;
import org.springframework.batch.item.xml.builder.StaxEventItemWriterBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.oxm.xstream.XStreamMarshaller;

import javax.sql.DataSource;
import java.util.Collections;

@Configuration
@EnableBatchProcessing
public class BatchConfig extends DefaultBatchConfigurer {
    @Autowired
    public JobBuilderFactory jobBuilderFactory;
    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Value("input/inputData*.csv")
    private Resource[] inputResources;

    @Bean
    public FlatFileItemReader<User> userFlatFileItemReader() {
        //Create reader instance
        FlatFileItemReader<User> reader = new FlatFileItemReader<>();
        //reader.setResource(inputResources);
        //Set number of lines to skips. Use it if file has header rows.
        reader.setLinesToSkip(1);

        //Configure how each line will be parsed and mapped to different values
        //LineMapper converts a String line into an Object
        reader.setLineMapper(new DefaultLineMapper<User>() {
            {
                //LineTokenizer an abstraction for turning a line of input into a FieldSet
                setLineTokenizer(new DelimitedLineTokenizer() {
                    {

                        //map ordered columns on proper object setter. Example 'firstValue' name tell to take value from
                        //first column and call setter for 'firstValue' field
                        setNames("firstValue", "secondValue", "thirdValue");
                    }
                });
                //FieldSetMapper takes a FieldSet object and maps its contents to an object
                setFieldSetMapper(new BeanWrapperFieldSetMapper<User>() {
                    {
                        setTargetType(User.class);
                    }
                });
            }
        });
        return reader;
    }

    @Bean
    public FlatFileItemReader<Profile> profileFlatFileItemReader() {
        //Create reader instance
        FlatFileItemReader<Profile> reader = new FlatFileItemReader<>();
        reader.setResource(new FileSystemResource("data/profileData.csv"));
        //Set number of lines to skips. Use it if file has header rows.
        reader.setLinesToSkip(1);

        //Configure how each line will be parsed and mapped to different values
        //LineMapper converts a String line into an Object
        reader.setLineMapper(new DefaultLineMapper<Profile>() {
            {
                //LineTokenizer an abstraction for turning a line of input into a FieldSet
                setLineTokenizer(new DelimitedLineTokenizer() {
                    {

                        //names of object field???

                        setNames("id", "email", "brand");
                    }
                });
                //FieldSetMapper takes a FieldSet object and maps its contents to an object
                setFieldSetMapper(new BeanWrapperFieldSetMapper<Profile>() {
                    {
                        setTargetType(Profile.class);
                    }
                });
            }
        });
        return reader;
    }

    @Bean
    public StaxEventItemWriter<Account> accountXmlItemWriter() {
        XStreamMarshaller accountMarshaller = new XStreamMarshaller();
        accountMarshaller.setAliases(Collections.singletonMap(
                "account",
                Account.class
        ));

        return new StaxEventItemWriterBuilder<Account>()
                .name("accountWriter")
                .resource(new FileSystemResource("data/accountData.xml"))
                .marshaller(accountMarshaller)
                .rootTagName("accounts")
                .build();
    }

    @Bean
    public MultiResourceItemReader<User> multiResourceItemReader() {
        MultiResourceItemReader<User> resourceItemReader = new MultiResourceItemReader<>();
        resourceItemReader.setResources(inputResources);
        resourceItemReader.setDelegate(userFlatFileItemReader());
        return resourceItemReader;
    }

    @Bean
    public UserProfileProcessor userProfileProcessor() {
        return new UserProfileProcessor();
    }

    @Bean
    public ProfileAccountProcessor profileAccountProcessor() {
        return new ProfileAccountProcessor();
    }

    @Bean
    public FlatFileItemWriter<Profile> profileFlatFileItemWriter() {
        FlatFileItemWriter<Profile> writer = new FlatFileItemWriter<>();
        writer.setResource(new FileSystemResource("data/profileData.csv"));

        //All job repetitions should "append" to same output file
        //writer.setAppendAllowed(true);

        //Name field values sequence based on object properties
        writer.setLineAggregator(new DelimitedLineAggregator<Profile>() {
            {
                setDelimiter(",");
                setFieldExtractor(new BeanWrapperFieldExtractor<Profile>() {
                    {
                        setNames(new String[]{"id", "email", "brand"});
                    }
                });
            }
        });
        return writer;
    }

    @Bean
    public Job readCSVFilesJob() {
        return jobBuilderFactory
                .get("readCSVFilesJob")
                .incrementer(new RunIdIncrementer())
                .start(step1())
                .next(step2())
                .build();
    }

    @Bean
    public Step step1() {
        return stepBuilderFactory.get("step1")
                .<User, Profile>chunk(5)
                .reader(multiResourceItemReader())
                .processor(userProfileProcessor())
                .writer(profileFlatFileItemWriter())
                .build();
    }

    @Bean
    public Step step2() {
        return stepBuilderFactory.get("step2")
                .<Profile, Account>chunk(5)
                .reader(profileFlatFileItemReader())
                .processor(profileAccountProcessor())
                .writer(accountXmlItemWriter())
                .build();
    }

    @Override
    public void setDataSource(DataSource dataSource) {
        //This BatchConfigurer ignores any DataSource
    }
}
