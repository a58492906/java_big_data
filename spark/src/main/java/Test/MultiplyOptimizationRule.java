package Test;

import org.apache.spark.sql.catalyst.rules.Rule;
import org.apache.spark.internal.Logging;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.rules.Rule;
import org.apache.spark.sql.catalyst.expressions.*;

/**
 * @author xjm
 * @version 1.0
 * @date 2022-06-27 10:31
 */

public class MultiplyOptimizationRule extends Rule{


    @Override
    public Object apply(Object plan) {
      //  if(plan)
       // if right.isInstanceOf[Literal]&&
         //       right.asInstanceOf[Literal].value.asInstanceOf[Double]==1.0=>
        System.out.println("MyRule-优化规则生效：");
        return null;
    }
}
