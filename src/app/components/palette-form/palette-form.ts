import { Component, inject } from '@angular/core';
import { StateService } from '../../services/state.service';
import { FormField } from '@angular/forms/signals';
import { MatInputModule } from '@angular/material/input';
import { MatSliderModule } from '@angular/material/slider';

@Component({
  selector: 'pbu-palette-form',
  imports: [MatInputModule, MatSliderModule, FormField],
  templateUrl: './palette-form.html',
  styleUrl: './palette-form.css',
})
export class PaletteForm {
  stateService = inject(StateService)
  stateForm = this.stateService.stateForm

  formatFloat(value: number): string {
    return Math.round(value * 100) + '%'
  }
}