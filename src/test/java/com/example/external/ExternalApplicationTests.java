package com.example.external;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.modulith.core.ApplicationModules;

@SpringBootTest
class ExternalApplicationTests {

	@Test
	void contextLoads() {
	}

	@Test 
	void verifiesModuleStructure() {
		ApplicationModules.of(ExternalApplication.class).verify();
	}
}
