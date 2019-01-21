package com.cooking.recipe

object DataModel {

  case class Recipe(recipe_id:Int,
                    recipe_name:String,
                    description:String,
                    ingredients:List[String] ) {

    override def toString: String = {
      s"$recipe_id,$description,$recipe_name"
    }
  }

  case class Pair(pair:(String,String))
  case class IngredientRecipe(pair:(String,Int),recipe_name:String)

  case class RecipeIngredient(recipe_id:Int,ingredient:List[String])

  case class RecipeItem(recipe_id:Int,
                        ingredient:String,
                        active:Boolean,
                        created_date:String,
                        updated_date:String) {


    override def toString: String = {
      //s"$recipe_id,$recipe_name,$description,$ingredient,$active,$updated_date,$created_date"
      s"$recipe_id,$ingredient,$updated_date,$active,$created_date"
    }
  }

  case class RecipeEvent(recipe_id:Int,
                         recipe_name:String,
                         description:String,
                         ingredient:String,
                         active:Boolean,
                         created_date:String,
                         updated_date:String) {

    override def toString: String = {
      //s"$recipe_id,$recipe_name,$description,$ingredient,$active,$updated_date,$created_date"
      s"$recipe_id,$recipe_name,$description,$ingredient,$active,$updated_date,$created_date"
    }
  }


}
