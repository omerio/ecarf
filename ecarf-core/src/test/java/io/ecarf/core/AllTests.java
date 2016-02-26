package io.ecarf.core;

import io.ecarf.core.cloud.task.processor.reason.phase2.DoReasonTask6Test;
import io.ecarf.core.cloud.task.processor.reason.phase2.DoReasonTask7Test;
import io.ecarf.core.reason.rulebased.RuleTest;
import io.ecarf.core.reason.rulebased.query.QueryGeneratorTest;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({
    RuleTest.class, 
    QueryGeneratorTest.class,
    DoReasonTask6Test.class,
    DoReasonTask7Test.class
})
public class AllTests {

}
