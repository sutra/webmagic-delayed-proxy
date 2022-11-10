package org.oxerr.webmagic.proxy;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.lang3.RandomUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.proxy.Proxy;
import us.codecraft.webmagic.proxy.ProxyProvider;

public class DelayedProxyProvider implements ProxyProvider, Externalizable {

	private final transient Logger log = LogManager.getLogger();

	private final transient DelayQueue<DelayedProxy> proxies;

	private final transient Map<Proxy, DelayedProxy> allProxies;

	private long minSuccessDelay;

	private long maxSuccessDelay;

	private long minFailureDelay;

	private long maxFailureDelay;

	private long waitTimeout;

	public DelayedProxyProvider() {
		this(Duration.ZERO, Duration.ZERO, Duration.ZERO, Duration.ZERO);
	}

	public DelayedProxyProvider(
		Duration minSuccessDelay,
		Duration maxSuccessDelay,
		Duration minFailureDelay,
		Duration maxFailureDelay
	) {
		this(
			minSuccessDelay,
			maxSuccessDelay,
			minFailureDelay,
			maxFailureDelay,
			Duration.ZERO
		);
	}

	public DelayedProxyProvider(
		Duration minSuccessDelay,
		Duration maxSuccessDelay,
		Duration minFailureDelay,
		Duration maxFailureDelay,
		Duration waitTimeout
	) {
		this.proxies = new DelayQueue<>();
		this.allProxies = new HashMap<>();

		final TimeUnit unit = TimeUnit.MILLISECONDS;

		this.minSuccessDelay = unit.convert(minSuccessDelay);
		this.maxSuccessDelay = unit.convert(maxSuccessDelay);

		this.minFailureDelay = unit.convert(minFailureDelay);
		this.maxFailureDelay = unit.convert(maxFailureDelay);

		this.waitTimeout = unit.convert(waitTimeout);
	}

	@Override
	public void returnProxy(Proxy proxy, Page page, Task task) {
		final DelayedProxy delayedProxy = this.allProxies.get(proxy);
		final boolean success = this.isSuccess(proxy, page, task);

		if (success) {
			delayedProxy.incrementAndGetSuccessCount();
		} else {
			delayedProxy.incrementAndGetFailureCount();
		}

		final Duration delay = this.getDelay(delayedProxy, page, task, success);
		delayedProxy.setAvailableTime(Instant.now().plus(delay));

		this.proxies.put(delayedProxy);

		this.printInfo();
	}

	@Override
	public Proxy getProxy(Task task) {
		final Proxy proxy;

		this.printInfo();

		try {
			if (this.waitTimeout > 0) {
				DelayedProxy dp = this.proxies.poll(this.waitTimeout, TimeUnit.MILLISECONDS);

				if (dp != null) {
					proxy = dp.getProxy();
				} else {
					log.warn("Wait for proxy timed out.");
					proxy = null;
				}
			} else {
				proxy = this.proxies.take().getProxy();
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException(e);
		}

		return proxy;
	}

	public synchronized void put(Proxy proxy) {
		if (!this.allProxies.containsKey(proxy)) {
			log.trace("Put proxy: {}.", proxy);

			final DelayedProxy delayedProxy = new DelayedProxy(proxy);
			this.allProxies.put(proxy, delayedProxy);
			this.proxies.put(delayedProxy);

			this.printInfo();
		} else {
			log.trace("Skipping put proxy: {}.", proxy);
		}
	}

	public DelayQueue<DelayedProxy> getProxies() {
		return proxies;
	}

	public Map<Proxy, DelayedProxy> getAllProxies() {
		return Collections.unmodifiableMap(allProxies);
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		List<DelayedProxy> delayedProxies = new ArrayList<>(this.allProxies.values());
		out.writeObject(delayedProxies);
		out.writeLong(this.minSuccessDelay);
		out.writeLong(this.maxSuccessDelay);
		out.writeLong(this.minFailureDelay);
		out.writeLong(this.maxFailureDelay);
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		@SuppressWarnings("unchecked")
		List<DelayedProxy> delayedProxies = (List<DelayedProxy>) in.readObject();
		delayedProxies.forEach(dp -> this.allProxies.put(dp.getProxy(), dp));
		this.minSuccessDelay = in.readLong();
		this.maxSuccessDelay = in.readLong();
		this.minFailureDelay = in.readLong();
		this.maxFailureDelay = in.readLong();
		this.proxies.addAll(this.allProxies.values());
	}

	protected Duration getDelay(DelayedProxy delayedProxy, Page page, Task task, boolean success) {
		final long minDelay;
		final long maxDelay;

		if (success) {
			minDelay = this.minSuccessDelay;
			maxDelay = this.maxSuccessDelay;
		} else {
			minDelay = this.minFailureDelay;
			maxDelay = this.maxFailureDelay;
		}

		final float delayFactor = this.getDelayFactor(delayedProxy, page, task, success);
		final float amount = RandomUtils.nextLong(minDelay, maxDelay) * delayFactor;
		final Duration delay = Duration.ofMillis((long) amount);

		log.trace("Proxy: {}, success count: {}, failure count: {}, delayFactor: {} delay: {}",
			delayedProxy::getProxy,
			delayedProxy::getSuccessCount,
			delayedProxy::getFailureCount,
			() -> delayFactor,
			() -> delay
		);

		return delay;
	}

	protected float getDelayFactor(DelayedProxy delayedProxy, Page page, Task task, boolean success) {
		final long totalCount = delayedProxy.getSuccessCount() + delayedProxy.getFailureCount();
		final float failureRate = totalCount != 0 ? (float) delayedProxy.getFailureCount() / (float) totalCount : 0;
		return 1 + failureRate * delayedProxy.getFailureCount();
	}

	protected boolean isSuccess(Proxy proxy, Page page, Task task) {
		boolean success = page.isDownloadSuccess()
			&& page.getStatusCode() >= 100
			&& page.getStatusCode() < 500;
		log.trace("{} is {}.", proxy, success ? "success" : "failure");
		return success;
	}

	protected void printInfo() {
		if (log.isTraceEnabled()) {
			String prefix = String.format("%1$32s\t%2$32s\t%3$8s\t%4$8s\t%5$16s%6$s", "Proxy", "Available Time", "Success", "Failure", "Delayed(ms)", System.lineSeparator());
			String stat = this.proxies.stream().sorted()
				.map(p -> String.format("%1$32s\t%2$32s\t%3$8d\t%4$8d\t%5$16d", p.getProxy(), p.getAvailableTime(), p.getSuccessCount(), p.getFailureCount(), p.getDelay(TimeUnit.MILLISECONDS)))
				.collect(Collectors.joining(System.lineSeparator(), prefix, ""));
			log.trace("\nAll proxy count: {}, proxy queue size: {}, expired delay available count: {}.\n{}",
				this.allProxies.size(),
				this.proxies.size(),
				this.proxies.stream().filter(p -> p.getDelay(TimeUnit.MILLISECONDS) <= 0).count(),
				stat
			);
		}
	}

}
