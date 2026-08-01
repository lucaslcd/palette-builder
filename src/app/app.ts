import { Component, signal } from '@angular/core';
import { ColorWheel } from './components/color-wheel/color-wheel';
import { ColorForm } from './components/color-form/color-form';
import { PaletteExport } from './components/palette-export/palette-export';

@Component({
  selector: 'app-root',
  imports: [ColorWheel, ColorForm, PaletteExport],
  templateUrl: './app.html',
  styleUrl: './app.css'
})
export class App {
  protected readonly title = signal('palette-builder');
}
