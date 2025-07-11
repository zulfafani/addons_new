/** @odoo-module **/
import { ListController } from "@web/views/list/list_controller";
import { registry } from "@web/core/registry";
import { listView } from "@web/views/list/list_view";

class GenerateReportListController extends ListController {
  renderButtons($node) {
    super.renderButtons($node);
    // Tidak perlu tambahan, template QWeb akan menambahkan tombol
  }
  onGenerateReport(event) {
    event.preventDefault();
    this.actionService.doAction({
      type: "ir.actions.act_window",
      res_model: "wizard.generate.stock.ledger",  // bisa wizard, atau langsung server action
      name: "Generate Stock Ledger Report",
      view_mode: "form",
      views: [[false, "form"]],
      target: "new",
    });
  }
}

registry.category("views").add("generate_report_button", {
  ...listView,
  Controller: GenerateReportListController,
  buttonTemplate: "integrasi_pos.ListViewButtonsExt",
});
