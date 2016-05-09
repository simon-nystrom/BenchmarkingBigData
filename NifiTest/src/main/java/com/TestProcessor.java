package com;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.reporting.ReportingInitializationContext;
import org.apache.nifi.reporting.ReportingTask;

import java.util.Collection;
import java.util.List;

public class TestProcessor implements ReportingTask {

    @Override
    public void initialize(ReportingInitializationContext reportingInitializationContext) throws InitializationException {

    }

    @Override
    public void onTrigger(ReportingContext reportingContext) {

    }

    @Override
    public Collection<ValidationResult> validate(ValidationContext validationContext) {
        return null;
    }

    @Override
    public PropertyDescriptor getPropertyDescriptor(String s) {
        return null;
    }

    @Override
    public void onPropertyModified(PropertyDescriptor propertyDescriptor, String s, String s1) {

    }

    @Override
    public List<PropertyDescriptor> getPropertyDescriptors() {
        return null;
    }

    @Override
    public String getIdentifier() {
        return null;
    }
}