package cn.sharesdk.analysis.web.anno;

import java.lang.annotation.*;

@Documented
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface ACTION {
    public abstract String path();
}
