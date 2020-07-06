package org.oxerr.webmagic.proxy;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.builder.CompareToBuilder;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import us.codecraft.webmagic.proxy.Proxy;

public class DelayedProxy implements Delayed, Externalizable {

	private Proxy proxy;

	private Instant availableTime;

	private AtomicLong successCount;

	private AtomicLong failureCount;

	public DelayedProxy() {
		this(null);
	}

	public DelayedProxy(Proxy proxy) {
		this(proxy, Duration.ZERO);
	}

	public DelayedProxy(Proxy proxy, Duration duration) {
		this.proxy = proxy;
		this.availableTime = Instant.now().plus(duration);
		this.successCount = new AtomicLong();
		this.failureCount = new AtomicLong();
	}

	public Proxy getProxy() {
		return proxy;
	}

	public void setProxy(Proxy proxy) {
		this.proxy = proxy;
	}

	public Instant getAvailableTime() {
		return availableTime;
	}

	public void setAvailableTime(Instant availableTime) {
		this.availableTime = availableTime;
	}

	public long getSuccessCount() {
		return successCount.longValue();
	}

	public long incrementAndGetSuccessCount() {
		return successCount.incrementAndGet();
	}

	public long getFailureCount() {
		return failureCount.longValue();
	}

	public long incrementAndGetFailureCount() {
		return this.failureCount.incrementAndGet();
	}

	@Override
	public long getDelay(TimeUnit unit) {
		Instant now = Instant.now();
		Duration duration = Duration.between(now, this.availableTime);
		return unit.convert(duration);
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeObject(proxy.toURI());
		out.writeObject(availableTime);
		out.writeObject(successCount);
		out.writeObject(failureCount);
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		URI uri = (URI) in.readObject();
		this.proxy = Proxy.create(uri);
		this.availableTime = (Instant) in.readObject();
		this.successCount = (AtomicLong) in.readObject();
		this.failureCount = (AtomicLong) in.readObject();
	}

	@Override
	public int compareTo(Delayed o) {
		DelayedProxy that = (DelayedProxy) o;
		return new CompareToBuilder()
			.append(this.proxy, that.proxy)
			.append(this.availableTime, that.availableTime)
			.toComparison();
	}

	@Override
	public int hashCode() {
		return new HashCodeBuilder(17, 37)
			.append(this.proxy)
			.append(this.availableTime)
			.toHashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}
		if (obj == this) {
			return true;
		}
		if (obj.getClass() != getClass()) {
			return false;
		}
		DelayedProxy that = (DelayedProxy) obj;
		return new EqualsBuilder()
			.append(this.proxy, that.proxy)
			.append(this.availableTime, that.availableTime)
			.isEquals();
	}

}
