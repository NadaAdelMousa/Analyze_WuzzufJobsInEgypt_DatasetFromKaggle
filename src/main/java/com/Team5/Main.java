package com.Team5;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

import java.util.Arrays;
import org.springframework.web.servlet.config.annotation.ViewControllerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@SpringBootApplication
public class Main implements WebMvcConfigurer {
    
    @Override
    public void addViewControllers(ViewControllerRegistry registry)
    {
		registry.addViewController("/home").setViewName("home");
    }
    public static void main(String[] args) {
        
        ApplicationContext applicationContext = SpringApplication.run(Main.class, args);
        String[] beanNames = applicationContext.getBeanDefinitionNames();
        Arrays.sort(beanNames);
        for (String beanName : beanNames)
            System.out.println(beanName);
    }
}
