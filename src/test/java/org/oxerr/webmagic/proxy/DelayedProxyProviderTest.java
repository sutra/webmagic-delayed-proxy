package org.oxerr.webmagic.proxy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import org.apache.commons.lang3.SerializationUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.proxy.Proxy;

class DelayedProxyProviderTest {

	private DelayedProxyProvider provider;

	@BeforeEach
	void setUp() throws Exception {
		this.provider = new DelayedProxyProvider();
		this.provider.put(new Proxy("127.0.0.1", 3128));
	}

	@Test
	void testReturnProxy() {
		var task = mock(Task.class);
		var page = mock(Page.class);
		var proxy = this.provider.getProxy(task);
		assertEquals(1, this.provider.getAllProxies().size());
		assertEquals(0, this.provider.getProxies().size());
		this.provider.returnProxy(proxy, page, task);
		assertEquals(1, this.provider.getAllProxies().size());
		assertEquals(1, this.provider.getProxies().size());
	}

	@Test
	void testGetProxy() {
		var proxy = provider.getProxy(null);
		assertEquals(new Proxy("127.0.0.1", 3128), proxy);
	}

	@Test
	void testPut() {
		provider.put(new Proxy("127.0.0.1", 3128));
		provider.put(new Proxy("127.0.0.1", 1028, "socks"));
		assertEquals(2, provider.getAllProxies().size());
		assertEquals(2, provider.getProxies().size());
	}

	@Test
	void testExternalizable() {
		var deserialized = SerializationUtils.roundtrip(this.provider);
		assertEquals(1, deserialized.getAllProxies().size());
		assertEquals(1, deserialized.getProxies().size());
	}

}
