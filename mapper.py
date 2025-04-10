from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def get_product_category_pairs(
        products_df: DataFrame,
        categories_df: DataFrame,
        product_category_links_df: DataFrame
) -> DataFrame:
    """
    Returns all product-category pairs and products without categories.

    Args:
        products_df: DataFrame with columns ['product_id', 'product_name']
        categories_df: DataFrame with columns ['category_id', 'category_name']
        product_category_links_df: DataFrame with columns ['product_id', 'category_id']

    Returns:
        DataFrame with columns ['product_name', 'category_name'] where
        category_name can be null for products without categories
    """
    # Join products with categories through linking table
    result_df = (
        products_df
        .join(product_category_links_df, "product_id", "left")
        .join(categories_df, "category_id", "left")
        .select(
            col("product_name"),
            col("category_name")
        )
        .distinct()
    )

    return result_df