package com.haofei.ctrl;

import com.haofei.service.DataService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Random;

@RestController
public class DataCtrl {
    @Autowired
    private DataService dataService ;

    @RequestMapping("/table")
    public String tableChecking(String tableName){
        System.out.println(tableName);
        return dataService.getTableCount(tableName);
    }

    @RequestMapping("/row")
    public List<String> rowChecking(String tableName){
        System.out.println(tableName);
        return dataService.getRowData(tableName);
    }

    @RequestMapping("/check")
    public void selfChecking(Integer seconds){
        String[] tableList =
                {"gold_lottery_flow","gold_score_flow","guide_step_flow","inc_equipment_flow","inc_prop_flow","level_up_flow","liveness_flow","login_flow","logout_flow","lottery_flow","mall_exchange_flow","money_flow","pay_success_flow","pay_value_flow","phoenix_raffle_flow","player_module_statistics_flow","relief_fund_flow","room_enter_flow","room_quit_flow","small_game_award","small_game_trigger","spec_fish_record","switch_equipment_flow","task_flow","upgrade_cannon_flow","user_gang_contrib_flow","user_mall_exchange_flow","vip_raffle_flow","add_mail_flow","add_reward_flow","auto_buy_prop_flow","bankruptcy_flow","baoji_flow","count_activity","ctrl_new_user_change_flow","dec_prop_flow","diamond_flow","dish_change_flow","energy_flow","fish_flow","gold_lottery_flow","gold_score_flow","guide_step_flow","inc_prop_flow","join_gang","level_up_flow","liveness_flow","login_flow","logout_flow","lottery_flow","mall_exchange_flow","money_flow","pay_value_flow","phoenix_raffle_flow","player_module_statistics_flow","relief_fund_flow","room_enter_flow","room_quit_flow","small_game_award","small_game_trigger","spec_fish_record","switch_equipment_flow","task_flow","upgrade_cannon_flow","user_gang_contrib_flow","user_mall_exchange_flow","vip_raffle_flow","add_mail_flow","add_reward_flow","auto_buy_prop_flow","bankruptcy_flow","baoji_flow","count_activity","dec_equipment_flow","dec_prop_flow","delete_user_express_reward_flow","diamond_flow","dish_change_flow","energy_flow","fish_flow","gang_contrib_flow","gold_lottery_flow","gold_score_flow","guide_step_flow","inc_prop_flow","level_up_flow","liveness_flow","login_flow","logout_flow","lottery_flow","mall_exchange_flow","money_flow","pay_order_result_flow","pay_success_flow","pay_value_flow","phoenix_raffle_flow","player_module_statistics_flow","relief_fund_flow","room_enter_flow","room_quit_flow","sea_soul_score_flow","small_game_award","small_game_trigger","spec_fish_record","switch_equipment_flow","task_flow","touch_fish_flow","update_actvity_number","upgrade_cannon_flow","user_gang_contrib_flow","user_mall_exchange_flow","vip_raffle_flow","add_mail_flow                                                    ","add_reward_flow","auto_buy_prop_flow","bankruptcy_flow","baoji_flow","bind_phone_flow","count_activity","ctrl_limit_change_flow","ctrl_new_user_change_flow","dec_equipment_flow","dec_prop_flow","delete_mail_flow","delete_user_drift_bottle_flow","delete_user_express_reward_flow"};
        String tableName = "";
        while (true){
            try {
                tableName = tableList[new Random().nextInt(tableList.length)];
                System.out.println(tableName + " | " + dataService.getTableCount(tableName));
                // System.out.println(dataService.getRowData(tableName).toString());
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                Thread.sleep(seconds*60*1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
