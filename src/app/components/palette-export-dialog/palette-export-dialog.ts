import { Component, inject, model } from '@angular/core';
import { MatButtonModule } from '@angular/material/button';
import { MAT_DIALOG_DATA, MatDialogActions, MatDialogClose, MatDialogContent, MatDialogTitle } from '@angular/material/dialog';
import { PaletteModel } from '../../services/color.state';
import { MatFormField, MatLabel, MatOption, MatSelect } from '@angular/material/select';
import { ExportState } from '../../services/export.state';
import { FormField } from '@angular/forms/signals';
import { MatCheckbox } from '@angular/material/checkbox';

@Component({
  selector: 'pbu-palette-export-dialog',
  imports: [
    MatButtonModule,
    MatDialogTitle,
    MatDialogContent,
    MatDialogActions,
    MatDialogClose,
    MatSelect,
    MatOption,
    MatFormField,
    FormField,
    MatLabel,
    MatCheckbox
  ],
  templateUrl: './palette-export-dialog.html',
  styleUrl: './palette-export-dialog.css',
})
export class PaletteExportDialog {
  data = inject<{palette: PaletteModel}>(MAT_DIALOG_DATA);
  palette = model(this.data.palette);

  exportState = inject(ExportState)
  exportForm = this.exportState.stateForm
}
