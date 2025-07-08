{% macro raise_user_defined_exception(p_exception_message) %}

    {#
/*
Purpose : Macro to raise user defined exception with the given message
*/
#}
    {% if execute %}
        {% set v_exception_msg = "Exception Occured: " + p_exception_message %}
        {{ exceptions.raise_compiler_error(v_exception_msg) }}
        {{ return(v_exception_msg) }}
    {% endif %}

{% endmacro %}