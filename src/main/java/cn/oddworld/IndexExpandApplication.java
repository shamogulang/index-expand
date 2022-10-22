package cn.oddworld;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class IndexExpandApplication {

    public static void main(String[] args) {
        SpringApplication app = new SpringApplication(IndexExpandApplication.class);
        app.run(args);
    }
}
